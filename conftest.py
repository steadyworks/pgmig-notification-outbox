"""Root-level conftest — runs at pytest collection start, BEFORE any test
module is imported.

Purpose: apply migrations + regenerate db/data_models/* so that DAL test
files which do things like `from db.data_models import JobStatus` at their
module scope can resolve symbols the agent's migration introduced.

Pytest flow:
  1. Pytest discovers rootdir = /app (via pytest.ini).
  2. Pytest loads /app/conftest.py — THIS FILE — at module level. Its
     top-level code runs here, synchronously, before any collection.
  3. Pytest then walks test paths, loads per-dir conftests, and imports
     each test module.

If we deferred this to a fixture (even session-scoped), (3) would happen
BEFORE our fixture ran — and any `from db.data_models import <new symbol>`
at module scope would ImportError during collection, aborting the run.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

_APP = Path("/app")
_HARNESS = _APP / "_harness_codegen"
_MIGRATIONS = _APP / "db" / "migrations"
_DATA_MODELS = _APP / "db" / "data_models"

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgres://photobook:photobook@localhost:5432/photobook?sslmode=disable",
)
SUPERUSER_URL = "postgres://postgres@localhost:5432/postgres?sslmode=disable"


def _log(msg: str) -> None:
    sys.stderr.write(f"[root-conftest] {msg}\n")


def _start_postgres() -> None:
    if subprocess.run(["pg_isready", "-U", "postgres"], capture_output=True).returncode == 0:
        return
    subprocess.run(["pg_ctlcluster", "16", "main", "start"], check=False)
    for _ in range(60):
        if subprocess.run(["pg_isready", "-U", "postgres"], capture_output=True).returncode == 0:
            return
        time.sleep(0.5)
    raise RuntimeError("postgres did not become ready within 30s")


def _ensure_role_and_db() -> None:
    import psycopg
    with psycopg.connect(SUPERUSER_URL, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'photobook'")
        if cur.fetchone() is None:
            cur.execute(
                "CREATE ROLE photobook WITH LOGIN SUPERUSER PASSWORD 'photobook'"
            )
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'photobook'")
        if cur.fetchone() is None:
            cur.execute("CREATE DATABASE photobook OWNER photobook")


def _ensure_auth_stub() -> None:
    import psycopg
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute("GRANT ALL ON SCHEMA public TO photobook")
        cur.execute("GRANT ALL ON SCHEMA public TO public")
        cur.execute("DROP SCHEMA IF EXISTS auth CASCADE")
        cur.execute("CREATE SCHEMA auth")
        cur.execute("GRANT USAGE ON SCHEMA auth TO photobook")
        cur.execute(
            "CREATE TABLE auth.users ("
            "  id uuid PRIMARY KEY,"
            "  email text, phone text,"
            "  email_confirmed_at timestamptz,"
            "  phone_confirmed_at timestamptz,"
            "  raw_user_meta_data jsonb"
            ")"
        )
        cur.execute("GRANT ALL ON auth.users TO photobook")


def _env() -> dict:
    env = os.environ.copy()
    env["DATABASE_URL"] = DATABASE_URL
    env["PYTHONPATH"] = str(_APP) + (
        os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env else ""
    )
    return env


def _run(cmd: list[str], *, cwd: Path) -> None:
    result = subprocess.run(cmd, env=_env(), cwd=str(cwd), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"{cmd[:2]} failed (rc={result.returncode})\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
        )


def _dbmate_up() -> None:
    _run(["dbmate", "--migrations-dir", str(_MIGRATIONS), "up"], cwd=_APP)


def _dbmate_dump() -> None:
    _run(["dbmate", "--migrations-dir", str(_MIGRATIONS), "dump"], cwd=_APP)


def _regenerate_codegen() -> None:
    gen_sqlmodel = _HARNESS / "generate_sqlmodel_from_sql.py"
    gen_crud = _HARNESS / "generate_crud_schemas.py"
    if not gen_sqlmodel.exists():
        return
    if not _DATA_MODELS.exists():
        return
    _run(["python", str(gen_sqlmodel)], cwd=_APP)
    if gen_crud.exists():
        (_APP / "db" / "externals").mkdir(parents=True, exist_ok=True)
        _run(["python", str(gen_crud)], cwd=_APP)


# One-time session setup: pg up → schema + auth stub → dbmate up.
#
# NOTE: we deliberately DO NOT regenerate db/data_models/ here, even though
# that was tempting. The shipped data_models/ at oracle is already correct
# (author committed it alongside the migration). The HEAD codegen we
# vendor in _harness_codegen/ produces output that silently omits some
# table classes (e.g. Assets, Jobs) when applied against dbmate's dumped
# schema.sql — root-causing that requires more investigation than it's
# worth right now. Consequence: DAL tests that do
# `from db.data_models import <new_symbol>` at module scope will fail at
# nop for agent-eval tasks that introduce a new symbol. Those tests
# register as collection errors and effectively become f2p signal.
def _session_bootstrap() -> None:
    if not _MIGRATIONS.exists():
        # Very early-era task that predates db/migrations/.
        return
    _log("starting postgres + auth stub + dbmate up")
    _start_postgres()
    _ensure_role_and_db()
    _ensure_auth_stub()
    _dbmate_up()
    _log("bootstrap complete")


_session_bootstrap()
