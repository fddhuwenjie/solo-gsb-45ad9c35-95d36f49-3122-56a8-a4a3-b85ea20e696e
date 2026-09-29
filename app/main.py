"""航标闪光巡检判定服务（FastAPI 应用入口）。"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Path, Query, Response

from . import config
from .align import AidPattern, judge
from .db import (
    BatchConflict,
    BatchNotFound,
    ConsumerConflict,
    Database,
    VersionConflict,
    VersionNotFound,
    VersionStateError,
)
from .models import (
    BatchAckRequest,
    BatchUploadRequest,
    ConsumerRegisterRequest,
    JudgeRequest,
    LibraryImportRequest,
    PatrolAnalyzeRequest,
    VersionCreateRequest,
)
from .patrol import PatrolWaypoint, analyze_patrol


def create_app(db_path: str | None = None) -> FastAPI:
    app = FastAPI(
        title="航标闪光巡检判定服务",
        description=(
            "联合估计观测时钟的相位与线性钟漂，对跨多个周期的闪光观测序列做循环对齐，"
            "按「漏记+误记总数 → 残差总和 → |钟漂| → 航标编号」裁决最佳航标。"
            "巡检整体核对把一次巡检的多个观测窗口用动态规划全局分配到计划航线航点。"
            "服务端特征库以不可变版本发布：草稿提交后可查询增删改差异，"
            "激活时单事务校验 base_version 仍是当前版本，冲突返回 409。"
            "现场设备按链式批次上传观测：批次摘要以前一批摘要链接，"
            "单事务校验并追加，相同载荷重放返回原结果，冲突返回 409 及期望值。"
            "多下游处理方先登记为必需消费者，再以 consumer_id/sequence/batch_digest "
            "确认批次（乱序、并发、重复安全）；设备 safe_sequence 取各消费者连续"
            "水位最小值，给出可安全归档的连续前缀。"
        ),
        version="1.5.0",
    )
    app.state.db = Database(db_path or config.db_path())

    @app.get("/", include_in_schema=False)
    def root() -> dict:
        return {"service": "navlight-inspector", "docs": "/docs", "health": "/health"}

    @app.get("/health", summary="健康检查")
    def health() -> dict:
        return {"status": "ok"}

    # ---------------- 特征库：版本发布 ----------------

    @app.post(
        "/library/versions",
        status_code=201,
        summary="创建特征库版本草稿",
        description="提交完整特征库及 base_version，服务端保存不可变草稿并计算增删改。",
    )
    def create_library_version(req: VersionCreateRequest) -> dict:
        try:
            return app.state.db.create_version(
                req.aids, base_version=req.base_version, source="api"
            )
        except VersionNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except VersionStateError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @app.get("/library/versions", summary="列出特征库版本")
    def list_library_versions() -> dict:
        versions = app.state.db.list_versions()
        return {"count": len(versions), "versions": versions}

    @app.get("/library/versions/{version_id}", summary="查看版本详情（含完整快照）")
    def get_library_version(version_id: int) -> dict:
        meta = app.state.db.get_version(version_id)
        if meta is None:
            raise HTTPException(status_code=404, detail="version not found")
        meta["aids"] = app.state.db.get_version_aids(version_id)
        return meta

    @app.get(
        "/library/versions/{version_id}/diff",
        summary="查询版本相对 base_version 的增删改",
    )
    def get_library_version_diff(version_id: int) -> dict:
        meta = app.state.db.get_version(version_id)
        if meta is None:
            raise HTTPException(status_code=404, detail="version not found")
        return {
            "version_id": version_id,
            "base_version": meta["base_version"],
            "diff": meta["diff"],
        }

    @app.post(
        "/library/versions/{version_id}/activate",
        summary="激活版本草稿",
        description=(
            "单个 SQLite 事务内确认草稿的 base_version 仍是当前激活版本："
            "冲突返回 409，成功后原子切换（旧激活版本转为 historical）。"
        ),
    )
    def activate_library_version(version_id: int) -> dict:
        try:
            return app.state.db.activate_version(version_id)
        except VersionNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except VersionStateError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except VersionConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.post(
        "/library/import",
        summary="导入 JSON 航标特征库（原子发布新版本）",
        description=(
            "兼容旧接口：mode=merge 按编号并入当前版本，mode=replace 清空重导；"
            "基于当前激活版本在单个事务内创建并直接激活新版本。"
        ),
    )
    def import_library(req: LibraryImportRequest) -> dict:
        imported, version = app.state.db.publish_import(
            req.aids, replace=req.mode == "replace"
        )
        return {
            "mode": req.mode,
            "imported": imported,
            "total": version["aid_count"],
            "version": version,
        }

    @app.get("/library", summary="查看当前激活特征库")
    def get_library() -> dict:
        version_id, aids = app.state.db.get_active_snapshot()
        return {
            "library_version": version_id,
            "count": len(aids),
            "aids": aids,
        }

    # ---------------- 判定 ----------------

    @app.post("/judge", summary="判定闪光观测序列")
    def judge_observations(req: JudgeRequest) -> dict:
        if req.library is not None:
            patterns = [AidPattern(a.aid_id, a.period, tuple(a.flashes)) for a in req.library]
            library_source = "inline"
            library_version = None
        else:
            # 请求开始即固定激活版本：同一只读读快照，计算期间即使发生版本切换也不受影响
            library_version, stored = app.state.db.get_active_snapshot()
            patterns = [
                AidPattern(r["aid_id"], r["period"], tuple(r["flashes"])) for r in stored
            ]
            library_source = "stored"

        best, candidates, reason = judge(
            patterns,
            req.observations,
            tolerance=req.tolerance,
            max_drift=req.max_drift,
            max_missed=req.max_missed,
            max_false=req.max_false,
            grid_max_steps=config.grid_max_steps(),
            seed_top_k=config.seed_top_k(),
        )

        response = {
            "status": "matched" if best is not None else "no_candidate",
            "reason": reason,
            "library_source": library_source,
            "library_version": library_version,
            "observation_count": len(req.observations),
            "parameters": {
                "tolerance": req.tolerance,
                "max_drift": req.max_drift,
                "max_missed": req.max_missed,
                "max_false": req.max_false,
            },
            "best": best.to_detail() if best is not None else None,
            "candidates": [c.to_summary() for c in candidates],
        }
        judgment_id = app.state.db.save_judgment(
            status=response["status"],
            best_aid_id=best.aid_id if best is not None else None,
            matched=best.matched if best is not None else 0,
            missed=best.missed if best is not None else 0,
            false_count=best.false if best is not None else 0,
            drift=best.drift if best is not None else None,
            library_version=library_version,
            request=req.model_dump(),
            response=response,
        )
        response["judgment_id"] = judgment_id
        return response

    # ---------------- 巡检整体核对 ----------------

    @app.post(
        "/patrols/analyze",
        summary="整体核对一次巡检的观测窗口与计划航线",
        description=(
            "先为每个观测窗口计算全部合格航标候选，再用动态规划把窗口一对一分配到"
            "严格递增的航点（可跳过窗口或航点，禁止逐窗局部最佳）；航点的 aid_id "
            "必须存在于请求开始时固定的激活特征库版本。"
        ),
    )
    def analyze_patrol_route(req: PatrolAnalyzeRequest) -> dict:
        # 请求开始即固定激活版本：同一只读快照，计算期间版本切换不影响本次核对
        library_version, stored = app.state.db.get_active_snapshot()
        patterns = {
            r["aid_id"]: AidPattern(r["aid_id"], r["period"], tuple(r["flashes"]))
            for r in stored
        }
        unknown = sorted({wp.aid_id for wp in req.waypoints} - patterns.keys())
        if unknown:
            if library_version is None:
                detail = "no active library version: import aids via POST /library/import first"
            else:
                detail = (
                    f"aid_id not found in active library version "
                    f"{library_version}: {', '.join(unknown)}"
                )
            raise HTTPException(status_code=400, detail=detail)

        waypoints = [PatrolWaypoint(wp.waypoint_id, wp.aid_id) for wp in req.waypoints]
        result = analyze_patrol(
            waypoints,
            [w.observations for w in req.windows],
            patterns,
            tolerance=req.tolerance,
            max_drift=req.max_drift,
            max_missed=req.max_missed,
            max_false=req.max_false,
            grid_max_steps=config.grid_max_steps(),
            seed_top_k=config.seed_top_k(),
        )

        assigned_windows = {a.window_index for a in result.assignments}
        assigned_waypoints = {a.waypoint_index for a in result.assignments}
        if result.assignments:
            # 漏巡点：首末已分配航点之间未被分配的航点（范围外未覆盖的不计入）
            first, last = min(assigned_waypoints), max(assigned_waypoints)
            missed = [j for j in range(first, last + 1) if j not in assigned_waypoints]
        else:
            missed = []

        # 航标 → 航线上的航点下标（候选证据中标注候选可用于哪些航点）
        aid_waypoints: dict[str, list[int]] = {}
        for j, wp in enumerate(req.waypoints):
            aid_waypoints.setdefault(wp.aid_id, []).append(j)

        return {
            "status": "assigned" if result.assignments else "no_assignment",
            "reason": result.reason,
            "library_version": library_version,
            "window_count": len(req.windows),
            "waypoint_count": len(req.waypoints),
            "parameters": {
                "tolerance": req.tolerance,
                "max_drift": req.max_drift,
                "max_missed": req.max_missed,
                "max_false": req.max_false,
            },
            "assignments": [
                {
                    "window_index": a.window_index,
                    "waypoint_index": a.waypoint_index,
                    "waypoint_id": req.waypoints[a.waypoint_index].waypoint_id,
                    "aid_id": req.waypoints[a.waypoint_index].aid_id,
                    "candidate": a.candidate.to_detail(),
                }
                for a in result.assignments
            ],
            "unassigned_windows": [
                {
                    "window_index": i,
                    "observation_count": len(req.windows[i].observations),
                }
                for i in range(len(req.windows))
                if i not in assigned_windows
            ],
            "missed_waypoints": [
                {
                    "waypoint_index": j,
                    "waypoint_id": req.waypoints[j].waypoint_id,
                    "aid_id": req.waypoints[j].aid_id,
                }
                for j in missed
            ],
            "window_candidates": [
                {
                    "window_index": i,
                    "candidates": [
                        {**cand.to_summary(), "waypoint_indices": aid_waypoints[aid_id]}
                        for aid_id, cand in sorted(
                            result.qualified[i].items(),
                            key=lambda kv: kv[1].ranking_key(),
                        )
                    ],
                }
                for i in range(len(req.windows))
            ],
        }

    @app.get("/judgments", summary="查询判定记录（按时间倒序）")
    def list_judgments(
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> dict:
        rows = app.state.db.list_judgments(limit=limit, offset=offset)
        return {"count": len(rows), "judgments": rows}

    @app.get("/judgments/{judgment_id}", summary="查询判定记录详情")
    def get_judgment(judgment_id: int) -> dict:
        row = app.state.db.get_judgment(judgment_id)
        if row is None:
            raise HTTPException(status_code=404, detail="judgment not found")
        return row

    # ---------------- 现场设备批次链 ----------------

    @app.post(
        "/field-devices/{device_id}/batches",
        status_code=201,
        summary="上传现场设备观测批次（链式追加，只保存不判定）",
        description=(
            "批次 sequence 从 1 递增，previous_digest 引用当前链头（首批为空）；"
            "服务端对排除摘要字段的规范化载荷计算 SHA-256 作为批次摘要。"
            "校验、追加记录、更新链头在单个 SQLite 事务内完成：相同载荷重放"
            "在幂等事务内原子累加 replay_count 并更新游标最近事件（不新增批次、"
            "不推进链头、不改写首次响应），返回原结果（status=replayed，HTTP 200）；"
            "载荷变化或链头过期返回 409 及期望值"
            "（expected_sequence / expected_previous_digest）。"
        ),
    )
    def upload_device_batch(
        req: BatchUploadRequest,
        response: Response,
        device_id: str = Path(min_length=1, max_length=128),
    ) -> dict:
        try:
            body, accepted = app.state.db.submit_batch(device_id, req)
        except BatchConflict as exc:
            raise HTTPException(status_code=409, detail=exc.detail)
        if not accepted:
            response.status_code = 200
        return body

    @app.get(
        "/field-devices/{device_id}/cursor",
        summary="查询设备批次链游标（链头、下一批期望值与最近事件）",
    )
    def get_device_cursor(device_id: str = Path(min_length=1, max_length=128)) -> dict:
        return app.state.db.get_device_cursor(device_id)

    @app.get(
        "/field-devices/{device_id}/batches",
        summary="按游标分页查询设备批次（首次受理状态 + 重放计数）",
    )
    def list_device_batches(
        device_id: str = Path(min_length=1, max_length=128),
        after_sequence: int = Query(0, ge=0, description="起始游标：返回 sequence 大于该值的批次"),
        limit: int = Query(50, ge=1, le=200),
    ) -> dict:
        return app.state.db.list_device_batches(
            device_id, after_sequence=after_sequence, limit=limit
        )

    @app.get(
        "/field-devices/{device_id}/batches/{sequence}",
        summary="查询单条批次详情（观测时间戳与判定阈值）",
    )
    def get_device_batch(
        device_id: str = Path(min_length=1, max_length=128),
        sequence: int = Path(ge=1),
    ) -> dict:
        row = app.state.db.get_device_batch(device_id, sequence)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        return row

    # ---------------- 多下游消费者：登记 / 确认 / 归档水位 ----------------

    @app.post(
        "/field-devices/{device_id}/consumers",
        status_code=201,
        summary="登记设备的必需下游消费者（不可删除）",
        description=(
            "登记后该消费者成为设备的必需消费者，参与 safe_sequence 最小值计算；"
            "消费者只能登记、不能删除。重复登记同一 consumer_id 幂等"
            "（HTTP 200，status=already_registered）。"
        ),
    )
    def register_device_consumer(
        req: ConsumerRegisterRequest,
        response: Response,
        device_id: str = Path(min_length=1, max_length=128),
    ) -> dict:
        body, created = app.state.db.register_consumer(device_id, req.consumer_id)
        if not created:
            response.status_code = 200
        return body

    @app.post(
        "/field-devices/{device_id}/acknowledgments",
        status_code=201,
        summary="下游消费者确认已处理一个批次（允许乱序、并发、重复）",
        description=(
            "确认携带 consumer_id、sequence 与 batch_digest。仅接受已存在且"
            "摘要相符的批次：批次不存在返回 404；消费者未登记或摘要冲突返回 409"
            "（digest_mismatch 附 expected_digest）。相同确认幂等"
            "（HTTP 200，status=duplicate）；新确认 HTTP 201（status=confirmed）。"
            "稀疏确认与连续水位推进在单个 SQLite 事务内完成，并发确认不会丢失推进。"
        ),
    )
    def acknowledge_device_batch(
        req: BatchAckRequest,
        response: Response,
        device_id: str = Path(min_length=1, max_length=128),
    ) -> dict:
        try:
            body, created = app.state.db.acknowledge_batch(
                device_id, req.consumer_id, req.sequence, req.batch_digest
            )
        except ConsumerConflict as exc:
            raise HTTPException(status_code=409, detail=exc.detail)
        except BatchNotFound as exc:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "batch_not_found",
                    "message": str(exc),
                    "device_id": exc.device_id,
                    "sequence": exc.sequence,
                },
            )
        if not created:
            response.status_code = 200
        return body

    @app.get(
        "/field-devices/{device_id}/archive-status",
        summary="查询设备安全归档水位（各消费者水位、首个缺口与阻塞消费者）",
    )
    def get_device_archive_status(device_id: str = Path(min_length=1, max_length=128)) -> dict:
        return app.state.db.get_archive_status(device_id)

    return app


app = create_app()
