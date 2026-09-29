"""多下游消费者确认与安全归档水位测试。

覆盖：必需消费者登记（不可删除、重复登记幂等）、批次确认（仅接受存在且
摘要相符的批次、相同确认幂等、摘要冲突 409）、乱序/稀疏确认与连续水位推进、
safe_sequence 取各消费者水位最小值、状态查询（各水位/首个缺口/阻塞消费者）、
并发确认不丢推进，以及不删除原批次。
"""
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
    """上传 n 个链式批次，返回各批次摘要列表（下标 0 = seq1）。"""
    digests = []
    prev = ""
    for seq in range(1, n + 1):
        r = client.post(
            f"/field-devices/{device}/batches",
            json=batch(f"b-{seq}", seq, previous_digest=prev),
        )
        assert r.status_code == 201
        digests.append(r.json()["digest"])
        prev = digests[-1]
    return digests


def register(client, device, consumer_id):
    return client.post(
        f"/field-devices/{device}/consumers", json={"consumer_id": consumer_id}
    )


def ack(client, device, consumer_id, sequence, digest):
    return client.post(
        f"/field-devices/{device}/acknowledgments",
        json={"consumer_id": consumer_id, "sequence": sequence, "batch_digest": digest},
    )


def status(client, device):
    return client.get(f"/field-devices/{device}/archive-status").json()


# ---------------- 登记 ----------------


def test_consumer_registration_and_idempotency(client):
    r = register(client, DEVICE, "archiver")
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "registered"
    assert body["consumer_id"] == "archiver" and body["watermark"] == 0
    assert body["registered_at"] is not None

    st = status(client, DEVICE)
    assert st["consumer_count"] == 1
    assert st["consumers"] == [
        {
            "consumer_id": "archiver",
            "watermark": 0,
            "first_gap": None,
            "registered_at": body["registered_at"],
        }
    ]
    # 已登记消费者尚未确认任何批次：safe_sequence 为 0，first_gap=1，
    # 链上也没有批次时不把它列为阻塞者
    assert st["safe_sequence"] == 0 and st["first_gap"] == 1
    assert st["blocking_consumers"] == []

    # 重复登记幂等：200 already_registered，水位不变
    r2 = register(client, DEVICE, "archiver")
    assert r2.status_code == 200
    assert r2.json()["status"] == "already_registered"
    assert r2.json()["registered_at"] == body["registered_at"]
    assert status(client, DEVICE)["consumer_count"] == 1


def test_consumers_cannot_be_deleted(client):
    """没有删除接口：登记记录持久保留，持续参与 safe_sequence 最小值。"""
    register(client, DEVICE, "archiver")
    # 路径下不存在删除消费者的路由（DELETE 不被允许）
    r = client.delete(f"/field-devices/{DEVICE}/consumers/archiver")
    assert r.status_code in (405, 404)
    assert status(client, DEVICE)["consumer_count"] == 1


def test_register_before_batches_and_validation(client):
    # 设备与批次都不存在时也可登记
    r = register(client, "dev-fresh", "c1")
    assert r.status_code == 201
    st = status(client, "dev-fresh")
    assert st["head_sequence"] == 0 and st["safe_sequence"] == 0

    # consumer_id 不能为空
    assert register(client, DEVICE, "").status_code == 422
    # batch_digest / sequence 经模型校验
    r = client.post(
        f"/field-devices/{DEVICE}/acknowledgments",
        json={"consumer_id": "c", "sequence": 0, "batch_digest": "00" * 32},
    )
    assert r.status_code == 422
    r = client.post(
        f"/field-devices/{DEVICE}/acknowledgments",
        json={"consumer_id": "c", "sequence": 1, "batch_digest": "not-a-digest"},
    )
    assert r.status_code == 422


# ---------------- 确认：存在性 / 摘要 / 幂等 ----------------


def test_ack_requires_registered_consumer(client):
    digests = upload_chain(client, DEVICE, 1)
    r = ack(client, DEVICE, "ghost", 1, digests[0])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "consumer_not_registered"
    assert detail["consumer_id"] == "ghost"
    # 未登记不产生确认，safe_sequence 仍为 0（无消费者）
    assert status(client, DEVICE)["safe_sequence"] == 0


def test_ack_requires_existing_batch(client):
    register(client, DEVICE, "archiver")
    upload_chain(client, DEVICE, 1)
    r = ack(client, DEVICE, "archiver", 99, "00" * 32)
    assert r.status_code == 404
    detail = r.json()["detail"]
    assert detail["error"] == "batch_not_found"
    assert detail["sequence"] == 99
    st = status(client, DEVICE)
    assert st["consumers"][0]["watermark"] == 0
    assert st["safe_sequence"] == 0


def test_ack_digest_mismatch_conflict(client):
    digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    # 用 seq2 的摘要确认 seq1 → 409，附期望摘要
    r = ack(client, DEVICE, "archiver", 1, digests[1])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "digest_mismatch"
    assert detail["sequence"] == 1
    assert detail["provided_digest"] == digests[1]
    assert detail["expected_digest"] == digests[0]
    # 冲突不写入确认：水位仍为 0
    st = status(client, DEVICE)
    assert st["consumers"][0]["watermark"] == 0 and st["safe_sequence"] == 0


def test_ack_idempotent_same_confirmation(client):
    digests = upload_chain(client, DEVICE, 1)
    register(client, DEVICE, "archiver")
    r1 = ack(client, DEVICE, "archiver", 1, digests[0])
    assert r1.status_code == 201 and r1.json()["status"] == "confirmed"
    first = r1.json()
    assert first["watermark"] == 1 and first["safe_sequence"] == 1

    # 相同确认重复提交：200 duplicate，水位/safe 不变，confirmed_at 保留首次
    r2 = ack(client, DEVICE, "archiver", 1, digests[0])
    assert r2.status_code == 200 and r2.json()["status"] == "duplicate"
    assert r2.json()["watermark"] == 1 and r2.json()["safe_sequence"] == 1
    assert r2.json()["confirmed_at"] == first["confirmed_at"]

    st = status(client, DEVICE)
    assert st["safe_sequence"] == 1 and st["first_gap"] == 2
    assert st["consumers"][0]["watermark"] == 1
    assert st["blocking_consumers"] == []  # 已追平链头


def test_ack_repeated_after_many_duplicates(client):
    digests = upload_chain(client, DEVICE, 1)
    register(client, DEVICE, "archiver")
    ack(client, DEVICE, "archiver", 1, digests[0])
    for _ in range(5):
        r = ack(client, DEVICE, "archiver", 1, digests[0])
        assert r.status_code == 200 and r.json()["status"] == "duplicate"
    assert status(client, DEVICE)["safe_sequence"] == 1


# ---------------- 乱序 / 稀疏确认与水位推进 ----------------


def test_out_of_order_sparse_acks_advance_watermark_to_first_gap(client):
    digests = upload_chain(client, DEVICE, 5)
    register(client, DEVICE, "archiver")

    # 先确认 3、5：水位仍是 0（1 是首个缺口）
    for seq in (3, 5):
        r = ack(client, DEVICE, "archiver", seq, digests[seq - 1])
        assert r.status_code == 201 and r.json()["watermark"] == 0
    st = status(client, DEVICE)["consumers"][0]
    assert st["watermark"] == 0 and st["first_gap"] == 1

    # 补上 2：仍缺 1，水位 0，first_gap 仍是 1
    assert ack(client, DEVICE, "archiver", 2, digests[1]).json()["watermark"] == 0
    assert status(client, DEVICE)["consumers"][0]["first_gap"] == 1

    # 补上 1：连续越过 1,2,3，停在缺口 4（5 已确认但不连续）
    r = ack(client, DEVICE, "archiver", 1, digests[0])
    assert r.json()["watermark"] == 3
    cs = status(client, DEVICE)["consumers"][0]
    assert cs["watermark"] == 3 and cs["first_gap"] == 4

    # 补上 4：越过 4,5 到链头
    r = ack(client, DEVICE, "archiver", 4, digests[3])
    assert r.json()["watermark"] == 5
    assert status(client, DEVICE)["consumers"][0]["first_gap"] is None


def test_safe_sequence_is_min_of_consumer_watermarks(client):
    digests = upload_chain(client, DEVICE, 4)
    register(client, DEVICE, "archiver")
    register(client, DEVICE, "ml-pipeline")
    register(client, DEVICE, "backup")

    # archiver 确认 1,2；ml 确认 1；backup 未确认
    ack(client, DEVICE, "archiver", 1, digests[0])
    ack(client, DEVICE, "archiver", 2, digests[1])
    ack(client, DEVICE, "ml-pipeline", 1, digests[0])
    st = status(client, DEVICE)
    assert st["safe_sequence"] == 0  # backup 水位为 0
    assert st["first_gap"] == 1
    assert st["blocking_consumers"] == ["backup"]

    # backup 确认 1：最小值变为 1，ml 与 backup 并列阻塞
    ack(client, DEVICE, "backup", 1, digests[0])
    st = status(client, DEVICE)
    assert st["safe_sequence"] == 1 and st["first_gap"] == 2
    assert st["blocking_consumers"] == ["backup", "ml-pipeline"]

    # backup 再确认 2：只剩 ml 阻塞在 1
    ack(client, DEVICE, "backup", 2, digests[1])
    assert status(client, DEVICE)["blocking_consumers"] == ["ml-pipeline"]

    # 三者都补齐到 3、4
    for cid in ("archiver", "ml-pipeline", "backup"):
        for seq in (3, 4):
            ack(client, DEVICE, cid, seq, digests[seq - 1])
    # ml/backup 还缺 2
    ack(client, DEVICE, "ml-pipeline", 2, digests[1])
    ack(client, DEVICE, "backup", 2, digests[1])
    st = status(client, DEVICE)
    assert st["safe_sequence"] == 4 and st["first_gap"] == 5
    # 全部追平链头：没有阻塞消费者
    assert st["blocking_consumers"] == []
    watermarks = {c["consumer_id"]: c["watermark"] for c in st["consumers"]}
    assert watermarks == {"archiver": 4, "ml-pipeline": 4, "backup": 4}


def test_tie_blocking_consumers_listed(client):
    """多个消费者并列最小水位时全部列出；追平链头者不算阻塞。"""
    digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "a")
    register(client, DEVICE, "b")
    register(client, DEVICE, "c")
    ack(client, DEVICE, "a", 1, digests[0])
    ack(client, DEVICE, "b", 1, digests[0])
    # c 未确认 → 仅 c 阻塞
    assert status(client, DEVICE)["blocking_consumers"] == ["c"]
    ack(client, DEVICE, "c", 1, digests[0])
    st = status(client, DEVICE)
    assert st["safe_sequence"] == 1
    # 三者水位同为 1 且都落后链头(3) → 全部阻塞，按 consumer_id 排序
    assert st["blocking_consumers"] == ["a", "b", "c"]


def test_ack_batches_never_deleted(client):
    """确认推进只追加确认记录：原批次与游标查询保持原样。"""
    digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    ack(client, DEVICE, "archiver", 1, digests[0])
    ack(client, DEVICE, "archiver", 2, digests[1])

    lst = client.get(f"/field-devices/{DEVICE}/batches").json()
    assert lst["count"] == 2
    assert [b["sequence"] for b in lst["batches"]] == [1, 2]
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 2 and cur["batch_count"] == 2
    assert cur["head_digest"] == digests[1]
    # 批次详情仍可读
    detail = client.get(f"/field-devices/{DEVICE}/batches/1").json()
    assert detail["digest"] == digests[0]


def test_new_consumer_starts_at_zero_and_gates_safe_sequence(client):
    """链头已推进后才登记的消费者从水位 0 开始，立即把 safe_sequence 拉回 0。"""
    digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "early")
    for seq in range(1, 4):
        ack(client, DEVICE, "early", seq, digests[seq - 1])
    assert status(client, DEVICE)["safe_sequence"] == 3

    register(client, DEVICE, "late")
    st = status(client, DEVICE)
    assert st["safe_sequence"] == 0
    assert st["blocking_consumers"] == ["late"]
    # 新消费者乱序补齐
    ack(client, DEVICE, "late", 2, digests[1])
    assert status(client, DEVICE)["safe_sequence"] == 0  # 仍缺 1
    ack(client, DEVICE, "late", 1, digests[0])
    assert status(client, DEVICE)["safe_sequence"] == 2  # 缺 3
    ack(client, DEVICE, "late", 3, digests[2])
    assert status(client, DEVICE)["safe_sequence"] == 3


def test_consumers_are_per_device(client):
    """同名消费者在不同设备上独立登记、独立水位。"""
    d1 = upload_chain(client, "dev-a", 1)
    d2 = upload_chain(client, "dev-b", 1)
    register(client, "dev-a", "shared")
    register(client, "dev-b", "shared")
    ack(client, "dev-a", "shared", 1, d1[0])
    # dev-a 已确认 → safe=1；dev-b 的同名消费者未确认 → safe=0
    assert status(client, "dev-a")["safe_sequence"] == 1
    assert status(client, "dev-b")["safe_sequence"] == 0
    # dev-b 用 dev-a 的摘要确认 → 409（摘要按设备参与计算）
    r = ack(client, "dev-b", "shared", 1, d1[0])
    assert r.status_code == 409
    ack(client, "dev-b", "shared", 1, d2[0])
    assert status(client, "dev-b")["safe_sequence"] == 1


# ---------------- 并发：确认不丢失、水位不回退 ----------------


def test_concurrent_acks_lose_no_progress(client):
    """多消费者各自乱序并发确认：每个消费者的连续水位都推进到链头。"""
    n_batches = 30
    digests = upload_chain(client, DEVICE, n_batches)
    consumer_ids = ["archiver", "ml", "backup"]
    for cid in consumer_ids:
        register(client, DEVICE, cid)

    barrier = threading.Barrier(len(consumer_ids) * n_batches)
    errors = []
    err_lock = threading.Lock()

    def confirm(cid, seq):
        barrier.wait()
        r = ack(client, DEVICE, cid, seq, digests[seq - 1])
        if r.status_code not in (200, 201):
            with err_lock:
                errors.append((cid, seq, r.status_code, r.text))

    threads = []
    for cid in consumer_ids:
        for seq in range(1, n_batches + 1):
            t = threading.Thread(target=confirm, args=(cid, seq))
            threads.append(t)
    # 乱序启动不影响正确性；直接全部启动
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    st = status(client, DEVICE)
    assert st["safe_sequence"] == n_batches
    assert st["first_gap"] == n_batches + 1
    assert st["blocking_consumers"] == []
    assert all(c["watermark"] == n_batches for c in st["consumers"])


def test_concurrent_same_ack_idempotent(client):
    """同一消费者对同批次并发重复确认：恰好一次 confirmed，其余 duplicate。"""
    digests = upload_chain(client, DEVICE, 1)
    register(client, DEVICE, "archiver")

    n = 12
    barrier = threading.Barrier(n)
    results = []
    lock = threading.Lock()

    def confirm():
        barrier.wait()
        r = ack(client, DEVICE, "archiver", 1, digests[0])
        with lock:
            results.append((r.status_code, r.json()["status"]))

    threads = [threading.Thread(target=confirm) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count((201, "confirmed")) == 1
    assert results.count((200, "duplicate")) == n - 1
    st = status(client, DEVICE)
    assert st["consumers"][0]["watermark"] == 1 and st["safe_sequence"] == 1


def test_concurrent_watermark_gap_fill(client):
    """并发下缺口被填补时水位推进不丢失：先并发确认 2..N，再并发补 1。"""
    n_batches = 20
    digests = upload_chain(client, DEVICE, n_batches)
    register(client, DEVICE, "archiver")

    def parallel(seqs):
        barrier = threading.Barrier(len(seqs))
        rs = []
        lock = threading.Lock()

        def worker(seq):
            barrier.wait()
            r = ack(client, DEVICE, "archiver", seq, digests[seq - 1])
            with lock:
                rs.append(r.status_code)

        ts = [threading.Thread(target=worker, args=(s,)) for s in seqs]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        return rs

    # 2..N 全部先到（乱序）：水位必须停在 0
    rs = parallel(list(range(2, n_batches + 1)))
    assert all(code in (200, 201) for code in rs)
    assert status(client, DEVICE)["consumers"][0]["watermark"] == 0

    # 补上 1：水位一次性推进到链头，推进不得丢失
    rs = parallel([1])
    assert rs == [201]
    st = status(client, DEVICE)
    assert st["consumers"][0]["watermark"] == n_batches
    assert st["safe_sequence"] == n_batches


def test_concurrent_mismatch_never_overwrites_ack(client):
    """并发的错误摘要确认不得污染已存在确认，全部返回 409。"""
    digests = upload_chain(client, DEVICE, 1)
    register(client, DEVICE, "archiver")
    assert ack(client, DEVICE, "archiver", 1, digests[0]).status_code == 201

    n = 8
    barrier = threading.Barrier(n)
    codes = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        r = ack(client, DEVICE, "archiver", 1, "ab" * 32)
        with lock:
            codes.append(r.status_code)

    ts = [threading.Thread(target=worker) for _ in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert codes == [409] * n
    # 正确的重复确认仍是幂等成功
    r = ack(client, DEVICE, "archiver", 1, digests[0])
    assert r.status_code == 200 and r.json()["status"] == "duplicate"
    assert status(client, DEVICE)["safe_sequence"] == 1


def test_legacy_db_picks_up_new_tables(tmp_path):
    """旧库（无消费者表）启动后自动补表，登记/确认/水位可正常工作。"""
    import sqlite3

    db = tmp_path / "legacy.db"
    # 仅建一个与本特性无关的最小旧表，模拟旧版数据库文件
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE aids (aid_id TEXT PRIMARY KEY, period REAL, flashes TEXT)")
    conn.execute(
        "INSERT INTO aids VALUES ('CN-001', 10.0, '[0.0]')"
    )
    conn.commit()
    conn.close()

    app = create_app(db_path=str(db))
    with TestClient(app) as c:
        # 旧库迁移为版本 1；新消费者表由 CREATE TABLE IF NOT EXISTS 自动建立
        assert c.get("/library").json()["library_version"] == 1
        digests = upload_chain(c, DEVICE, 2)
        register(c, DEVICE, "archiver")
        r = ack(c, DEVICE, "archiver", 1, digests[0])
        assert r.status_code == 201 and r.json()["safe_sequence"] == 1
        st = status(c, DEVICE)
        assert st["consumers"][0]["watermark"] == 1
        ack(c, DEVICE, "archiver", 2, digests[1])
        assert status(c, DEVICE)["safe_sequence"] == 2
