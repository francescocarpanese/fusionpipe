"""
Migration: add visual subtree tables (node_subtrees, node_subtree_relation).

Run this script once against an existing database to add the two new tables
introduced in the feature/subtreecollapse branch.  The script is idempotent —
running it more than once is safe (IF NOT EXISTS guards).

Usage:
    python migration_scripts/migration_add_subtrees.py

The DATABASE_URL environment variable must be set (same format used by the app).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import psycopg2


def migrate(db_url: str) -> None:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS node_subtrees (
            subtree_id TEXT PRIMARY KEY,
            pipeline_id TEXT,
            tag TEXT DEFAULT NULL,
            collapsed BOOLEAN DEFAULT FALSE,
            pos_x DOUBLE PRECISION DEFAULT 0.0,
            pos_y DOUBLE PRECISION DEFAULT 0.0,
            width DOUBLE PRECISION DEFAULT 400.0,
            height DOUBLE PRECISION DEFAULT 300.0,
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(pipeline_id)
        )
    """)
    print("node_subtrees table ensured.")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS node_subtree_relation (
            node_id TEXT PRIMARY KEY,
            subtree_id TEXT,
            FOREIGN KEY (node_id) REFERENCES nodes(node_id),
            FOREIGN KEY (subtree_id) REFERENCES node_subtrees(subtree_id)
        )
    """)
    print("node_subtree_relation table ensured.")

    conn.commit()
    cur.close()
    conn.close()
    print("Migration complete.")


if __name__ == "__main__":
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)
    migrate(db_url)
