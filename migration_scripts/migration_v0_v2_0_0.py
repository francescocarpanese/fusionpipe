import psycopg2
import os
from src.fusionpipe.utils.db_utils import init_db
from collections import defaultdict

def migrate_database():
    """
    Migrate data from fusionpipe_prod1 (old structure) to fusionpipe_prod2 (new structure)
    """
    
    # Connect to both databases
    old_db_url = "dbname=fusionpipe_prod2 port=5432"
    new_db_url = "dbname=fusionpipe_prod3 port=5432"
    
    old_conn = psycopg2.connect(old_db_url)
    new_conn = psycopg2.connect(new_db_url)
    
    old_cur = old_conn.cursor()
    new_cur = new_conn.cursor()
    
    try:
        # Initialize the new database with the correct schema
        print("Initializing new database schema...")
        init_db(new_conn)
        
        # --  Migrate project table
        print("Migrating projects...")
        # Ensure "project1" exists in the projects table
        new_cur.execute(
            "INSERT INTO projects (project_id, tag) VALUES (%s, %s) ON CONFLICT (project_id) DO NOTHING",
            ("migration1", "migration1")
        )

        # -- Migrate pipelines table (no changes needed)
        print("Migrating pipelines...")
        old_cur.execute("SELECT pipeline_id, tag, owner, notes FROM pipelines")
        pipelines = old_cur.fetchall()
        
        for pipeline in pipelines:
            new_cur.execute(
                "INSERT INTO pipelines (pipeline_id, tag, owner, notes) VALUES (%s, %s, %s, %s) ON CONFLICT (pipeline_id) DO NOTHING",
                pipeline
            )
        # Initialise new fields in the pipelines table
        new_cur.execute("""
            UPDATE pipelines SET project_id = 'migration1', blocked = false
        """)

        # Migrate nodes table (structure changed - no node_tag in old version)
        print("Migrating nodes...")
        old_cur.execute("SELECT node_id, status, editable, notes, folder_path FROM nodes")
        nodes = old_cur.fetchall()
        
        for node in nodes:
            node_id, status, editable, notes, folder_path = node
            referenced = not editable  # Convert to boolean
            # Set node_tag to node_id as default (matching the new logic)
            new_cur.execute(
                "INSERT INTO nodes (node_id, status, referenced, notes, folder_path, node_tag) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (node_id) DO NOTHING",
                (node_id, status, referenced, notes, folder_path, node_id)
            )

        # Initialise new fields in the nodes table
        new_cur.execute("""
            UPDATE nodes SET blocked = false
        """)            
        
        # Check if old node_pipeline_relation has node_tag column
        old_cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'node_pipeline_relation'
        """)
        old_columns = [row[0] for row in old_cur.fetchall()]
        has_node_tag = 'node_tag' in old_columns

        # -- Migrate node_relation table with edge_id
        print("Migrating node_relation...")
        old_cur.execute("SELECT child_id, parent_id FROM node_relation")
        node_relations = old_cur.fetchall()

        # Group by child_id to assign edge_id per child

        child_edges = defaultdict(list)
        for child_id, parent_id in node_relations:
            child_edges[child_id].append(parent_id)

        for child_id, parents in child_edges.items():
            for idx, parent_id in enumerate(parents, start=1):
                edge_id = f"{idx:02d}"
                new_cur.execute(
                    "INSERT INTO node_relation (child_id, parent_id, edge_id) VALUES (%s, %s, %s)",
                    (child_id, parent_id, edge_id)
                )

        # -- Migrate node_pipeline_relation table
        print("Migrating node_pipeline_relation...")
        # Old structure had node_tag in node_pipeline_relation
        old_cur.execute("""
            SELECT node_id, pipeline_id, last_update, position_x, position_y 
            FROM node_pipeline_relation
        """)
        relations = old_cur.fetchall()
        
        for relation in relations:
            node_id, pipeline_id, last_update, position_x, position_y = relation
            
            # Insert into new node_pipeline_relation (without node_tag)
            new_cur.execute("""
                INSERT INTO node_pipeline_relation (node_id, pipeline_id, last_update, position_x, position_y) 
                VALUES (%s, %s, %s, %s, %s) 
                ON CONFLICT (node_id, pipeline_id) DO NOTHING
            """, (node_id, pipeline_id, last_update, position_x, position_y))
            

        # -- Pipeline relation doesn't need to be migrated
        
        
        # Commit all changes
        new_conn.commit()
        
        # Verify migration
        print("\nMigration completed! Verifying data...")
        
        # Check counts
        old_cur.execute("SELECT COUNT(*) FROM pipelines")
        old_pipelines_count = old_cur.fetchone()[0]
        
        old_cur.execute("SELECT COUNT(*) FROM nodes")
        old_nodes_count = old_cur.fetchone()[0]
        
        old_cur.execute("SELECT COUNT(*) FROM node_pipeline_relation")
        old_relations_count = old_cur.fetchone()[0]
        
        new_cur.execute("SELECT COUNT(*) FROM pipelines")
        new_pipelines_count = new_cur.fetchone()[0]
        
        new_cur.execute("SELECT COUNT(*) FROM nodes")
        new_nodes_count = new_cur.fetchone()[0]
        
        new_cur.execute("SELECT COUNT(*) FROM node_pipeline_relation")
        new_relations_count = new_cur.fetchone()[0]
        
        print(f"Pipelines: {old_pipelines_count} -> {new_pipelines_count}")
        print(f"Nodes: {old_nodes_count} -> {new_nodes_count}")
        print(f"Node-Pipeline Relations: {old_relations_count} -> {new_relations_count}")
        
        if old_pipelines_count == new_pipelines_count and old_nodes_count == new_nodes_count and old_relations_count == new_relations_count:
            print("✅ Migration successful - all data transferred!")
        else:
            print("⚠️ Warning: Data counts don't match. Please verify manually.")
            
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