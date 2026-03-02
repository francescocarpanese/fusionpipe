"""
Migration: add visual node group tables (node_groups, node_group_relation).

Replaces the earlier 'subtree' naming (node_subtrees / node_subtree_relation).

Scenarios handled:
  1. Fresh database with neither old nor new tables → creates new tables.
  2. Old subtree tables exist (from the previous migration_add_subtrees.py) →
     renames them and their primary-key columns to the new naming, then migrates
     any existing data.
  3. New node_groups tables already exist → skips creation (idempotent).

Run once against an existing or new database:
    python migration_scripts/migration_add_node_groups.py

DATABASE_URL environment variable must be set.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import psycopg2


def table_exists(cur, table_name: str) -> bool:
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = %s)",
        (table_name,),
    )
    return cur.fetchone()[0]


def migrate(db_url: str) -> None:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    has_old = table_exists(cur, "node_subtrees")
    has_new = table_exists(cur, "node_groups")

    if has_new:
        print("node_groups table already exists — nothing to do.")
        cur.close()
        conn.close()
        return

    if has_old:
        # ----------------------------------------------------------------
        # Migrate from old subtree naming to new node_group naming
        # ----------------------------------------------------------------
        print("Found old node_subtrees table — migrating to node_groups …")

        # Drop FK on node_subtree_relation first (needed before renaming parent)
        cur.execute("""
            ALTER TABLE node_subtree_relation
            DROP CONSTRAINT IF EXISTS node_subtree_relation_subtree_id_fkey
        """)

        # Rename relation table and its column
        cur.execute("ALTER TABLE node_subtree_relation RENAME TO node_group_relation")
        cur.execute(
            "ALTER TABLE node_group_relation RENAME COLUMN subtree_id TO group_id"
        )

        # Rename main table and its primary-key column
        cur.execute("ALTER TABLE node_subtrees RENAME TO node_groups")
        cur.execute("ALTER TABLE node_groups RENAME COLUMN subtree_id TO group_id")

        # Re-add the foreign key
        cur.execute("""
            ALTER TABLE node_group_relation
            ADD CONSTRAINT node_group_relation_group_id_fkey
            FOREIGN KEY (group_id) REFERENCES node_groups(group_id)
        """)

        print("Migration from node_subtrees → node_groups complete.")
    else:
        # ----------------------------------------------------------------
        # Fresh install: create both new tables from scratch
        # ----------------------------------------------------------------
        print("Creating node_groups table …")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS node_groups (
                group_id TEXT PRIMARY KEY,
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
        print("node_groups table created.")

        print("Creating node_group_relation table …")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS node_group_relation (
                node_id TEXT PRIMARY KEY,
                group_id TEXT,
                FOREIGN KEY (node_id) REFERENCES nodes(node_id),
                FOREIGN KEY (group_id) REFERENCES node_groups(group_id)
            )
        """)
        print("node_group_relation table created.")

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
