"""Engine / session factory. SQLite is the default; swap DATABASE_URL for Postgres."""
from __future__ import annotations

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from .models import Base


def make_engine(database_url: str) -> Engine:
    """Create an engine. For SQLite, enable WAL + a busy timeout so multiple
    worker processes can write concurrently without immediate 'database is
    locked' errors. (For real concurrency at scale, use Postgres.)"""
    is_sqlite = database_url.startswith("sqlite")
    engine = create_engine(
        database_url,
        future=True,
        # Allow use across the process; each worker builds its own engine anyway.
        connect_args={"check_same_thread": False} if is_sqlite else {},
    )

    if is_sqlite:

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragmas(dbapi_conn, _record):  # pragma: no cover - trivial
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA busy_timeout=5000;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.close()

    return engine


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(engine)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)
