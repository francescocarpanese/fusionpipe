import psycopg2
import os

def connect_to_db(db_url=os.environ.get("DATABASE_URL")):
    # Example db_url: "dbname=<yourdb> user=<youruser> password=<yourpassword> host=localhost port=<port>"
    conn = psycopg2.connect(db_url)
    return conn

def get_node_parents(cur, node_id):
    cur.execute('SELECT parent_id FROM node_relation WHERE child_id = %s', (node_id,))
    return [row[0] for row in cur.fetchall()]

def get_node_folder_path(cur, node_id):
    cur.execute('SELECT folder_path FROM nodes WHERE node_id = %s', (node_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_all_parent_node_folder_paths(node_id):
    """
    Get all parent node folder paths for a given node_id.
    """
    conn = connect_to_db()
    cur = conn.cursor()

    parent_paths = []
    parents = get_node_parents(cur, node_id)
    
    for parent_id in parents:
        path = get_node_folder_path(cur, parent_id)
        if path:
            parent_paths.append(path)
    
    return parent_paths

def get_node_id():
    """
    Get the node id, which is the name of the folder two levels up from this file.
    """
    current_file = os.path.abspath(__file__)
    two_levels_up = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_file))))
    node_id = os.path.basename(two_levels_up)
    return node_id