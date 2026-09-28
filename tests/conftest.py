"""pytest 全局准备：在导入应用前把默认数据库指向临时目录。

``app.main`` 在模块导入时即执行 ``app = create_app()``（会对默认路径的
SQLite 库跑首次迁移）。测试一律显式传 ``db_path=tmp_path``，这里再把
默认路径也重定向到临时目录，避免收集阶段误迁移仓库内的 ``data/navlight.db``。
"""
import os
import tempfile

os.environ.setdefault("NAVLIGHT_DB_PATH", os.path.join(tempfile.mkdtemp(), "navlight-test.db"))
