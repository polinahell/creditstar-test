import logging
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from config import config

logger = logging.getLogger(__name__)


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )


@contextmanager
def get_cursor(cursor_factory=psycopg2.extras.RealDictCursor):
    """Yield (cursor, connection). Commits on success, rolls back on error."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield cur, conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
