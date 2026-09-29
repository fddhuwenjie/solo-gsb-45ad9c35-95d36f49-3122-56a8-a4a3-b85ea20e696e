"""设备批次多处理方确认测试：消费者登记、稀疏确认/水位、幂等与 409、safe_sequence 与并发。"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

THRESHOLDS = {"tolerance": 0.5, "max_drift": 5e-4, "max_missed": 3, "max_false": 3}
DEVICE = "dev-01"


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def obs(n, base=1_700_000_000.0, step=0.5):
    return [
        {"local_id": f"o{i:03d}", "captured_at": base + i * step}
        for i in range(n)
    ]


def batch(batch_id, sequence, previous_digest="", observations=None, **kw):
    body = {
        "batch_id": batch_id,
        "sequence": sequence,
        "previous_digest": previous_digest,
        "observations": observations if observations is not None else obs(3),
        **THRESHOLDS,
    }
    body.update(kw)
    return body


def upload_chain(client, device, n):
    """上传 1..n 的链式批次，返回 {sequence: digest}。"""
    digests = {}
    prev = ""
    for i in range(1, n + 1):
        r = client.post(
            f"/field-devices/{device}/batches",
            json=batch(f"b-{i}", i, previous_digest=prev),
        )
        assert r.status_code == 201, r.text
        digests[i] = r.json()["digest"]
        prev = digests[i]
    return digests


def register(client, consumer, device=DEVICE):
    return client.post(
        f"/field-devices/{device}/consumers", json={"consumer_id": consumer}
    )


def confirm(client, consumer, sequence, digest, device=DEVICE):
    return client.post(
        f"/field-devices/{device}/confirmations",
        json={"consumer_id": consumer, "sequence": sequence, "batch_digest": digest},
    )


def status(client, device=DEVICE):
    return client.get(f"/field-devices/{device}/consumer-status")


# ---------------- 状态：无消费者 / 未知设备 ----------------


def test_status_unknown_device_is_zeroed(client):
    """未上传、未登记消费者的设备：safe_sequence=0、首个缺口 1、无阻塞消费者。"""
    s = status(client, "dev-never").json()
    assert s == {
        "device_id": "dev-never",
        "head_sequence": 0,
        "safe_sequence": 0,
        "first_gap": 1,
        "blocking_consumers": [],
        "consumer_count": 0,
        "consumers": [],
    }


def test_status_no_consumers_safe_zero_even_with_batches(client):
    upload_chain(client, DEVICE, 3)
    s = status(client).json()
    assert s["head_sequence"] == 3
    assert s["safe_sequence"] == 0  # 无消费者时为 0
    assert s["first_gap"] == 1
    assert s["consumers"] == [] and s["blocking_consumers"] == []


# ---------------- 消费者登记 ----------------


def test_register_consumer_and_idempotent_repeat(client):
    r = register(client, "archiver")
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "registered"
    assert body["consumer_id"] == "archiver" and body["device_id"] == DEVICE
    assert body["watermark"] == 0
    assert body["safe_sequence"] == 0 and body["consumer_count"] == 1
    assert body["registered_at"] is not None
    first_at = body["registered_at"]

    # 重复登记：HTTP 200、already_registered，首次登记时刻保留
    r = register(client, "archiver")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "already_registered"
    assert body["registered_at"] == first_at
    assert body["consumer_count"] == 1

    # 状态接口列出该消费者（水位 0、缺口 1）
    s = status(client).json()
    assert s["consumer_count"] == 1
    c0 = s["consumers"][0]
    assert c0["consumer_id"] == "archiver"
    assert c0["watermark"] == 0 and c0["first_gap"] == 1
    assert c0["registered_at"] == first_at


def test_register_multiple_consumers_and_no_delete(client):
    """登记多个必需消费者；不提供删除接口（DELETE 不被路由支持）。"""
    assert register(client, "a").status_code == 201
    assert register(client, "b").status_code == 201
    assert register(client, "c").status_code == 201
    s = status(client).json()
    assert [c["consumer_id"] for c in s["consumers"]] == ["a", "b", "c"]
    assert s["consumer_count"] == 3
    # 无删除接口：DELETE 请求不允许
    r = client.request(
        "DELETE", f"/field-devices/{DEVICE}/consumers/a"
    )
    assert r.status_code in (405, 404)
    # 消费者仍在
    assert status(client).json()["consumer_count"] == 3


def test_register_validation(client):
    assert register(client, "").status_code == 422
    assert client.post(
        f"/field-devices/{DEVICE}/consumers", json={"consumer_id": "x" * 129}
    ).status_code == 422
    assert client.post(
        f"/field-devices/{DEVICE}/consumers", json={}
    ).status_code == 422


def test_consumers_are_device_scoped(client):
    """同一 consumer_id 在不同设备上是各自独立的登记与水位。"""
    upload_chain(client, "dev-a", 1)
    upload_chain(client, "dev-b", 1)
    assert register(client, "worker", "dev-a").status_code == 201
    # dev-b 未登记：确认 404
    r = confirm(
        client, "worker", 1,
        client.get("/field-devices/dev-b/batches").json()["batches"][0]["digest"],
        device="dev-b",
    )
    assert r.status_code == 404
    # 登记 dev-b 后互不影响
    assert register(client, "worker", "dev-b").status_code == 201
    sa = status(client, "dev-a").json()
    sb = status(client, "dev-b").json()
    assert sa["consumers"][0]["watermark"] == sb["consumers"][0]["watermark"] == 0


# ---------------- 确认：错误前置校验 ----------------


def test_confirm_requires_registered_consumer(client):
    digests = upload_chain(client, DEVICE, 2)
    r = confirm(client, "ghost", 1, digests[1])
    assert r.status_code == 404
    detail = r.json()["detail"]
    assert "not registered" in detail
    # 水位/确认表无副作用
    assert status(client).json()["consumers"] == []


def test_confirm_requires_existing_batch(client):
    register(client, "a")
    upload_chain(client, DEVICE, 2)
    # 未上传的序号 → 404
    r = confirm(client, "a", 3, "00" * 32)
    assert r.status_code == 404
    assert "not found" in r.json()["detail"]


def test_confirm_wrong_digest_is_409_with_expected(client):
    digests = upload_chain(client, DEVICE, 2)
    register(client, "a")
    r = confirm(client, "a", 1, "ab" * 32)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "ack_conflict"
    assert detail["reason"] == "batch_digest_mismatch"
    assert detail["expected_digest"] == digests[1]
    # 冲突确认不落库：水位仍为 0
    c0 = status(client).json()["consumers"][0]
    assert c0["watermark"] == 0 and c0["first_gap"] == 1


def test_confirm_validation_errors(client):
    register(client, "a")
    upload_chain(client, DEVICE, 1)
    base = f"/field-devices/{DEVICE}/confirmations"
    assert client.post(base, json={"consumer_id": "a", "sequence": 0,
                                  "batch_digest": "00" * 32}).status_code == 422
    assert client.post(base, json={"consumer_id": "a", "sequence": 1,
                                  "batch_digest": "xyz"}).status_code == 422
    assert client.post(base, json={"consumer_id": "a", "sequence": 1,
                                  "batch_digest": "0" * 63}).status_code == 422
    assert client.post(base, json={"consumer_id": "a", "sequence": 1}).status_code == 422
    assert client.post(base, json={"sequence": 1,
                                  "batch_digest": "00" * 32}).status_code == 422


# ---------------- 连续水位与稀疏乱序 ----------------


def test_in_order_confirms_advance_watermark(client):
    digests = upload_chain(client, DEVICE, 3)
    register(client, "a")

    r = confirm(client, "a", 1, digests[1])
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "confirmed"
    assert body["consumer_id"] == "a" and body["sequence"] == 1
    assert body["batch_digest"] == digests[1]
    assert body["watermark"] == 1
    assert body["safe_sequence"] == 1
    assert body["first_gap"] == 2
    assert body["acked_at"] is not None

    r = confirm(client, "a", 2, digests[2])
    assert r.json()["watermark"] == 2 and r.json()["safe_sequence"] == 2

    r = confirm(client, "a", 3, digests[3])
    assert r.json()["watermark"] == 3 and r.json()["safe_sequence"] == 3
    # 追平链头：不再阻塞
    s = status(client).json()
    c0 = s["consumers"][0]
    assert c0["blocking"] is False and c0["first_gap"] == 4
    assert s["blocking_consumers"] == []
    assert s["first_gap"] == 4


def test_out_of_order_sparse_confirms_then_gap_fill(client):
    """乱序确认 2、3 不推进水位；补齐 1 后水位一次跃到 3。"""
    digests = upload_chain(client, DEVICE, 3)
    register(client, "a")

    assert confirm(client, "a", 2, digests[2]).json()["watermark"] == 0
    assert confirm(client, "a", 3, digests[3]).json()["watermark"] == 0
    s = status(client).json()
    c0 = s["consumers"][0]
    assert c0["watermark"] == 0 and c0["first_gap"] == 1
    # 缺口 1 是已存在批次 → 阻塞
    assert c0["blocking"] is True
    assert s["blocking_consumers"] == ["a"]
    assert s["first_gap"] == 1 and s["safe_sequence"] == 0

    r = confirm(client, "a", 1, digests[1])
    assert r.json()["watermark"] == 3  # 由稀疏集合重算，跃过已补的 2、3
    assert r.json()["safe_sequence"] == 3


def test_gap_blocks_until_filled_and_far_ahead_ack_kept(client):
    """确认 1 后跳到 4：水位停在 1，稀疏确认保留；补 2、3 后继续推进。"""
    digests = upload_chain(client, DEVICE, 4)
    register(client, "a")
    confirm(client, "a", 1, digests[1])
    assert confirm(client, "a", 4, digests[4]).json()["watermark"] == 1
    assert confirm(client, "a", 2, digests[2]).json()["watermark"] == 2
    assert confirm(client, "a", 3, digests[3]).json()["watermark"] == 4
    s = status(client).json()
    assert s["safe_sequence"] == 4 and s["blocking_consumers"] == []


def test_duplicate_confirmation_idempotent(client):
    """相同确认重放：200 already_confirmed，幂等不写状态、不改确认时刻。"""
    digests = upload_chain(client, DEVICE, 2)
    register(client, "a")
    r1 = confirm(client, "a", 1, digests[1])
    assert r1.status_code == 201
    acked_at = r1.json()["acked_at"]

    r2 = confirm(client, "a", 1, digests[1])
    assert r2.status_code == 200
    body = r2.json()
    assert body["status"] == "already_confirmed"
    assert body["watermark"] == 1
    assert body["acked_at"] == acked_at  # 保留首次确认时刻
    assert body["safe_sequence"] == 1

    # 乱序缺口后的重复确认同样幂等
    confirm(client, "a", 2, digests[2])
    assert confirm(client, "a", 2, digests[2]).status_code == 200
    assert status(client).json()["consumers"][0]["watermark"] == 2


def test_digest_conflict_on_reconfirm_returns_409(client):
    """同序号已确认后，用不同摘要重放 → 409，附既有/期望摘要，确认不可变。"""
    digests = upload_chain(client, DEVICE, 2)
    register(client, "a")
    assert confirm(client, "a", 1, digests[1]).status_code == 201

    r = confirm(client, "a", 1, digests[2])  # 序号 1 却给批次 2 的摘要
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "ack_digest_conflict"
    assert detail["expected_digest"] == digests[1]
    assert detail["existing_digest"] == digests[1]

    # 水位不受影响
    assert status(client).json()["consumers"][0]["watermark"] == 1


def test_digest_case_insensitive_normalized(client):
    """请求里大写十六进制摘要规范化为小写后与服务端摘要相符，正常接受。"""
    digests = upload_chain(client, DEVICE, 1)
    register(client, "a")
    r = confirm(client, "a", 1, digests[1].upper())
    assert r.status_code == 201, r.text
    assert r.json()["batch_digest"] == digests[1]


# ---------------- 多消费者：safe_sequence 取最小 ----------------


def test_safe_sequence_is_min_watermark_across_consumers(client):
    digests = upload_chain(client, DEVICE, 3)
    register(client, "fast")
    register(client, "slow")

    # fast 全部确认、slow 一个未确认：safe=0（任一消费者未确认前缀）
    for i in (1, 2, 3):
        confirm(client, "fast", i, digests[i])
    s = status(client).json()
    wm = {c["consumer_id"]: c for c in s["consumers"]}
    assert wm["fast"]["watermark"] == 3 and wm["slow"]["watermark"] == 0
    assert s["safe_sequence"] == 0
    assert s["first_gap"] == 1
    assert s["blocking_consumers"] == ["slow"]
    assert wm["fast"]["blocking"] is False
    assert wm["slow"]["blocking"] is True

    # slow 确认 1：safe=min(3,1)=1，共同缺口推进到 2
    confirm(client, "slow", 1, digests[1])
    s = status(client).json()
    assert s["safe_sequence"] == 1
    assert s["first_gap"] == 2
    wm = {c["consumer_id"]: c for c in s["consumers"]}
    assert wm["slow"]["first_gap"] == 2 and wm["slow"]["blocking"] is True

    # slow 补齐到 3：全部追平，safe=3，无阻塞
    for i in (2, 3):
        confirm(client, "slow", i, digests[i])
    s = status(client).json()
    assert s["safe_sequence"] == 3 and s["first_gap"] == 4
    assert s["blocking_consumers"] == []
    assert all(c["blocking"] is False for c in s["consumers"])


def test_late_registered_consumer_reduces_safe_sequence(client):
    """链已完整且旧消费者全确认后，新登记消费者会把 safe_sequence 拉回 0。"""
    digests = upload_chain(client, DEVICE, 3)
    register(client, "a")
    for i in (1, 2, 3):
        confirm(client, "a", i, digests[i])
    assert status(client).json()["safe_sequence"] == 3

    r = register(client, "b")
    assert r.json()["safe_sequence"] == 0
    s = status(client).json()
    assert s["safe_sequence"] == 0 and s["blocking_consumers"] == ["b"]

    # 新消费者补齐后恢复
    for i in (1, 2, 3):
        confirm(client, "b", i, digests[i])
    assert status(client).json()["safe_sequence"] == 3


def test_new_batches_extend_gap_for_caught_up_consumers(client):
    """消费者追平链头后，新批次到达：缺口重新落在已存在批次上，再次阻塞。"""
    digests = upload_chain(client, DEVICE, 1)
    register(client, "a")
    confirm(client, "a", 1, digests[1])
    s = status(client).json()
    assert s["safe_sequence"] == 1 and s["blocking_consumers"] == []

    # 再上传第 2 批
    r = client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-2", 2, previous_digest=digests[1]),
    )
    digests[2] = r.json()["digest"]
    s = status(client).json()
    assert s["head_sequence"] == 2
    assert s["safe_sequence"] == 1
    assert s["first_gap"] == 2
    assert s["blocking_consumers"] == ["a"]

    confirm(client, "a", 2, digests[2])
    assert status(client).json()["safe_sequence"] == 2


# ---------------- 原批次不被删除/改写 ----------------


def test_acknowledgements_never_modify_batches(client):
    digests = upload_chain(client, DEVICE, 2)
    register(client, "a")
    confirm(client, "a", 1, digests[1])
    confirm(client, "a", 1, digests[1])  # 幂等重放
    # 错误摘要确认不改变批次
    confirm(client, "a", 2, "00" * 32)

    lst = client.get(f"/field-devices/{DEVICE}/batches").json()
    assert lst["count"] == 2
    for item in lst["batches"]:
        assert item["status"] == "accepted" and item["replay_count"] == 0
    d1 = client.get(f"/field-devices/{DEVICE}/batches/1").json()
    assert d1["digest"] == digests[1] and d1["observations"] == obs(3)
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 2 and cur["head_digest"] == digests[2]


# ---------------- 并发：确认不丢、登记不重 ----------------


def test_concurrent_confirms_advance_without_lost_update(client):
    """两个消费者各自乱序并发确认全部批次：水位/safe_sequence 不丢推进。"""
    n = 12
    digests = upload_chain(client, DEVICE, n)
    register(client, "a")
    register(client, "b")

    errors = []
    lock = threading.Lock()

    def worker(cid, seq):
        r = confirm(client, cid, seq, digests[seq])
        if r.status_code != 201:
            with lock:
                errors.append((cid, seq, r.status_code, r.text[:120]))

    jobs = [(cid, i) for cid in ("a", "b") for i in range(1, n + 1)]
    threads = [threading.Thread(target=worker, args=j) for j in jobs]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []

    s = status(client).json()
    assert all(c["watermark"] == n for c in s["consumers"]), s
    assert s["safe_sequence"] == n
    assert s["first_gap"] == n + 1 and s["blocking_consumers"] == []


def test_concurrent_same_confirmation_all_idempotent(client):
    """同一确认并发重放：恰好一个 201，其余 200，只有一条稀疏记录。"""
    digests = upload_chain(client, DEVICE, 1)
    register(client, "a")

    n = 10
    barrier = threading.Barrier(n)
    codes = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        r = confirm(client, "a", 1, digests[1])
        with lock:
            codes.append((r.status_code, r.json()["status"]))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert codes.count((201, "confirmed")) == 1
    assert codes.count((200, "already_confirmed")) == n - 1
    s = status(client).json()
    assert s["consumers"][0]["watermark"] == 1 and s["safe_sequence"] == 1


def test_concurrent_registration_single_consumer(client):
    """并发登记同一消费者：恰好一个 201，其余 200，只有一行登记。"""
    n = 10
    barrier = threading.Barrier(n)
    codes = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        r = register(client, "once")
        with lock:
            codes.append(r.status_code)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert codes.count(201) == 1 and codes.count(200) == n - 1
    assert status(client).json()["consumer_count"] == 1
