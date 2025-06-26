import psycopg2
import os
import re

def connect_to_db(db_url=os.environ.get("DATABASE_URL")):
    # Example db_url: "dbname=<yourdb> user=<youruser> password=<yourpassword> host=localhost port=<port>"
    conn = psycopg2.connect(db_url)
    return conn

def get_node_parents_db(cur, node_id):
    cur.execute('SELECT parent_id FROM node_relation WHERE child_id = %s', (node_id,))
    return [row[0] for row in cur.fetchall()]

def get_node_folder_path_db(cur, node_id):
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
    parents = get_node_parents_db(cur, node_id)
    
    for parent_id in parents:
        path = get_node_folder_path_db(cur, parent_id)
        if path:
            parent_paths.append(path)
    
    return parent_paths

def get_node_id():
    """
    Get the node id by searching the current working directory path for a folder
    name matching the pattern: n_<14 digits>_<4 digits>.
    """
    pwd = os.getcwd()
    # Pattern: n_ followed by 14 digits, underscore, 4 digits
    pattern = re.compile(r'n_\d{14}_\d{4}')
    match = pattern.search(pwd)
    if match:
        return match.group(0)
    return None

def get_folder_path_node():
    """
    Get the folder path of the current node.
    """
    node_id = get_node_id()
    if not node_id:
        raise ValueError("Node ID could not be determined from the current working directory.")
    
    conn = connect_to_db()
    cur = conn.cursor()
    
    folder_path = get_node_folder_path_db(cur, node_id)
    
    cur.close()
    conn.close()
    
    return folder_path

def get_folder_path_code():
    """
    Get the code folder path of the current node.
    """
    node_folder_path = get_folder_path_node()
    if not node_folder_path:
        raise ValueError("Node folder path could not be determined.")
    
    # Assuming the code folder is a subfolder named 'code' within the node folder
    code_folder_path = os.path.join(node_folder_path, 'code')
    
    if not os.path.exists(code_folder_path):
        raise FileNotFoundError(f"Code folder does not exist: {code_folder_path}")
    
    return code_folder_path

def get_folder_path_data():
    """
    Get the data folder path of the current node.
    """
    node_folder_path = get_folder_path_node()
    if not node_folder_path:
        raise ValueError("Node folder path could not be determined.")
    
    # Assuming the data folder is a subfolder named 'data' within the node folder
    data_folder_path = os.path.join(node_folder_path, 'data')
    
    if not os.path.exists(data_folder_path):
        raise FileNotFoundError(f"Data folder does not exist: {data_folder_path}")
    
    return data_folder_path

def get_folder_path_reports():
    """
    Get the reports folder path of the current node.
    """
    node_folder_path = get_folder_path_node()
    if not node_folder_path:
        raise ValueError("Node folder path could not be determined.")
    
    # Assuming the reports folder is a subfolder named 'reports' within the node folder
    reports_folder_path = os.path.join(node_folder_path, 'reports')
    
    if not os.path.exists(reports_folder_path):
        raise FileNotFoundError(f"Reports folder does not exist: {reports_folder_path}")
    
    return reports_folder_path