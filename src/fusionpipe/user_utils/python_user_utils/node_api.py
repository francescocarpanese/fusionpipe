import psycopg2
import os
import re

def connect_to_db(db_url=os.environ.get("DATABASE_URL")):
    """
    Establish a connection to the PostgreSQL database using the provided database URL.

    Args:
        db_url (str): The database connection URL. Defaults to the 'DATABASE_URL' environment variable.

    Returns:
        psycopg2.extensions.connection: A connection object to the database.
    """
    conn = psycopg2.connect(db_url)
    return conn

def get_node_parents_db(cur, node_id):
    """
    Retrieve the parent node IDs for a given node from the database.

    Args:
        cur (psycopg2.extensions.cursor): Database cursor.
        node_id (str): The node ID whose parents are to be fetched.

    Returns:
        list: A list of parent node IDs.
    """
    cur.execute('SELECT parent_id FROM node_relation WHERE child_id = %s', (node_id,))
    return [row[0] for row in cur.fetchall()]

def get_node_folder_path_db(cur, node_id):
    """
    Retrieve the folder path for a given node ID from the database.

    Args:
        cur (psycopg2.extensions.cursor): Database cursor.
        node_id (str): The node ID whose folder path is to be fetched.

    Returns:
        str or None: The folder path if found, else None.
    """
    cur.execute('SELECT folder_path FROM nodes WHERE node_id = %s', (node_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_all_parent_node_folder_paths(node_id):
    """
    Get all parent node folder paths for a given node_id.

    Args:
        node_id (str): The node ID whose parent folder paths are to be fetched.

    Returns:
        list: A list of folder paths for all parent nodes.
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
    Get the node id by searching for a '.node_id' file in the current directory or its parent directories.

    Returns:
        str or None: The node ID if found, else None.
    """
    current_dir = os.getcwd()
    while True:
        node_id_file = os.path.join(current_dir, '.node_id')
        if os.path.isfile(node_id_file):
            with open(node_id_file, 'r') as f:
                node_id = f.read().strip()
                return node_id if node_id else None
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            break
        current_dir = parent_dir
    return None

def get_folder_path_node():
    """
    Get the folder path of the current node.

    Returns:
        str: The folder path of the current node.
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

    Returns:
        str: The path to the code folder.
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

    Returns:
        str: The path to the data folder.
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

    Returns:
        str: The path to the reports folder.
    """
    node_folder_path = get_folder_path_node()
    if not node_folder_path:
        raise ValueError("Node folder path could not be determined.")
    
    # Assuming the reports folder is a subfolder named 'reports' within the node folder
    reports_folder_path = os.path.join(node_folder_path, 'reports')
    
    if not os.path.exists(reports_folder_path):
        raise FileNotFoundError(f"Reports folder does not exist: {reports_folder_path}")
    
    return reports_folder_path