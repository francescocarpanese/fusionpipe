import psycopg2

# This is very important. During a migratio of the database, the entris which are SERIAL 
# in tables need to reset the index to the max value + 1 of the existing entries.
def reset_serial(cur_dst, table, col='id'):
    cur_dst.execute(f"""
    SELECT setval(
      pg_get_serial_sequence('{table}','{col}'),
      (SELECT COALESCE(MAX({col}),0) + 1 FROM {table}),
      false
    )
    """)


def migrate_database():
    """
    Migrate data from fusionpipe_prod3 (v2_0_0) to fusionpipe_prod4 (v2_1_0)
    - Set project_id in nodes based on the first pipeline the node belongs to.
    - Update folder_path to include project_id.
    """
    old_db_url = "dbname=fusionpipe_prod3 port=5432"
    new_db_url = "dbname=fusionpipe_prod4 port=5432"

    old_conn = psycopg2.connect(old_db_url)
    new_conn = psycopg2.connect(new_db_url)

    old_cur = old_conn.cursor()
    new_cur = new_conn.cursor()

    try:
        # --- Initialize new DB schema (should include project_id in nodes) ---
        from fusionpipe.utils.db_utils import init_db
        print("Initializing new database schema...")
        init_db(new_conn)

        # --- Copy all tables except nodes ---
        tables_to_copy = [
            "projects", "pipelines", "processes"
        ]
        for table in tables_to_copy:
            print(f"Migrating {table}...")
            old_cur.execute(f"SELECT * FROM {table}")
            rows = old_cur.fetchall()
            if not rows:
                continue
            columns = [desc[0] for desc in old_cur.description]
            colnames = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            for row in rows:
                new_cur.execute(
                    f"INSERT INTO {table} ({colnames}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    row
                )

        # --- Migrate nodes with new project_id and folder_path ---
        print("Migrating nodes with project_id and new folder_path...")
        old_cur.execute("SELECT node_id, status, referenced, notes, folder_path, node_tag, blocked FROM nodes")
        nodes = old_cur.fetchall()
        for node in nodes:
            node_id, status, referenced, notes, folder_path, node_tag, blocked = node

            # 1. Find first pipeline for this node
            old_cur.execute(
                "SELECT pipeline_id FROM node_pipeline_relation WHERE node_id = %s ORDER BY last_update ASC LIMIT 1",
                (node_id,)
            )
            result = old_cur.fetchone()
            project_id = None
            if result:
                pipeline_id = result[0]
                old_cur.execute("SELECT project_id FROM pipelines WHERE pipeline_id = %s", (pipeline_id,))
                project_row = old_cur.fetchone()
                if project_row:
                    project_id = project_row[0]

            # 2. Update folder_path: */node_id -> */project_id/node_id
            
            if folder_path and project_id:
                new_folder_path = f'/data/defuse/fusionpipe-data-1/{project_id}/{node_id}'
            else:
                new_folder_path = f'error project {project_id}, folder_path {folder_path}'

            # Insert into new DB
            new_cur.execute(
                """
                INSERT INTO nodes (node_id, status, referenced, notes, folder_path, node_tag, blocked, project_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (node_id) DO NOTHING
                """,
                (node_id, status, referenced, notes, new_folder_path, node_tag, blocked, project_id)
            )


        # --- Copy all tables except nodes ---
        tables_to_copy = [
            "node_pipeline_relation",
            "node_relation", "pipeline_relation",
        ]
        for table in tables_to_copy:
            print(f"Migrating {table}...")
            old_cur.execute(f"SELECT * FROM {table}")
            rows = old_cur.fetchall()
            if not rows:
                continue
            columns = [desc[0] for desc in old_cur.description]
            colnames = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            for row in rows:
                new_cur.execute(
                    f"INSERT INTO {table} ({colnames}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    row
                )
        
        # Need to reset serials for tables with serial primary keys
        reset_serial(new_cur, 'node_relation')
        reset_serial(new_cur, 'pipeline_relation')

        new_conn.commit()
        print("✅ Migration completed successfully.")

    except Exception as e:
        print(f"❌ Migration failed: {e}")
        new_conn.rollback()
        raise

    finally:
        old_cur.close()
        new_cur.close()
        old_conn.close()
        new_conn.close()

if __name__ == "__main__":
    migrate_database()