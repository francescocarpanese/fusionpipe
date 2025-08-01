import pytest
import os
import tempfile
import sqlite3

import psycopg2
import random
import string
import re
import shutil

DATABASE_URL_TEST = os.getenv('DATABASE_URL_TEST')

@pytest.fixture(scope="function")
def pg_test_db():
    # Connect to the default database as the test user
    admin_conn = psycopg2.connect(DATABASE_URL_TEST)
    admin_conn.autocommit = True
    admin_cur = admin_conn.cursor()

    # Generate random db name
    db_name = "test_fusionpipe_" + ''.join(random.choices(string.ascii_lowercase, k=8))
    admin_cur.execute(f"CREATE DATABASE {db_name};")

    # Build the test database URL by replacing dbname in DATABASE_URL_TEST
    test_db_url = re.sub(r"(dbname=)[^ ]+", rf"\1{db_name}", DATABASE_URL_TEST)
    test_conn = psycopg2.connect(test_db_url)
    yield test_conn

    # Cleanup: close and drop the test database
    test_conn.close()
    # Reconnect to the original database to drop the test db
    admin_conn2 = psycopg2.connect(DATABASE_URL_TEST)
    admin_conn2.autocommit = True
    admin_cur2 = admin_conn2.cursor()
    admin_cur2.execute(f"DROP DATABASE {db_name};")
    admin_cur2.close()
    admin_conn2.close()
    admin_cur.close()
    admin_conn.close()


@pytest.fixture
def tmp_base_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

@pytest.fixture
def tmp_database_path(tmp_base_dir):
    db_path = os.path.join(tmp_base_dir, "connection.db")
    yield db_path
    if os.path.exists(db_path):  # Cleanup after the test
        os.remove(db_path)

@pytest.fixture
def dag_dummy_1():
    import networkx as nx
    from fusionpipe.utils.pip_utils import NodeState 
    # Create a simple directed acyclic graph (DAG) for testing
    G = nx.DiGraph()
    G.add_edges_from([
        ("A", "B"),
        ("A", "C"),
        ("C", "D"),
    ])
    G.add_node("E")  # Add a node with no edges
    G.name = "12345"
    G.graph['pipeline_id'] = G.name
    G.graph['project_id'] = "test_project"
    G.graph['notes'] = "A simple test DAG"
    G.graph['tag'] = "test_tag"
    G.graph['owner'] = "test_group"
    G.graph['blocked'] = False

    # Add a 'status' attribute to each node using NodeState
    for node in G.nodes:
        G.nodes[node]['referenced'] = False
        G.nodes[node]['tag'] = 'test_tag'
        G.nodes[node]['folder_path'] = f'dummy_folder_path_{node}'
        G.nodes[node]['notes'] = 'test notes'
        G.nodes[node]['position'] = [0, 0]  # Default position
        if node == "A":
            G.nodes[node]['status'] = "ready"
        elif node == "B":
            G.nodes[node]['status'] = "running"
        elif node == "C":
            G.nodes[node]['status'] = "completed"
        elif node == "D":
            G.nodes[node]['status'] = "staledata"
        elif node == "E":
            G.nodes[node]['status'] = "ready"            
    return G


@pytest.fixture
def dag_dummy_project():
    import networkx as nx
    from fusionpipe.utils.pip_utils import NodeState 
    # Create a simple directed acyclic graph (DAG) for testing
    G = nx.DiGraph()
    G.add_edges_from([
        ("pip_1", "pip_2"),
        ("pip_1", "pip_3"),
        ("pip_3", "pip_4"),
    ])
    G.add_node("pip_5")  # Add a node with no edges
    G.name = "pro_12345"
    G.graph['project_id'] = "pro_12345"
    G.graph['notes'] = "A simple test project DAG"
    G.graph['tag'] = "test_tag"
    G.graph['owner'] = "test_group"

    # Add a 'status' attribute to each node using NodeState
    for node in G.nodes:
        G.nodes[node]['tag'] = 'test_tag' + node
        G.nodes[node]['notes'] = 'test notes'
        G.nodes[node]['blocked'] = False

    G.nodes['pip_5']['blocked'] = True  # pip_5 is blocked

    return G


@pytest.fixture
def dict_dummy_1():
    # Create a simple dictionary for testing identical to dag_dummy_1
    return {
        "pipeline_id": "12345",
        "notes": "A simple test DAG",
        "tag": "test_tag",
        "owner": "test_group",
        "project_id": "test_project",
        "blocked": False,
        "nodes": {
            "A": {"status": "ready", "referenced": False, "tag": 'test_tag', 'notes': 'test notes', 'parents': [], 'position': [0, 0], 'folder_path': 'dummy_folder_path_A'},
            "B": {"status": "running", "referenced": False, "tag": 'test_tag', 'notes': 'test notes', 'parents': ['A'], 'position': [0, 0], 'folder_path': 'dummy_folder_path_B'},
            "C": {"status": "completed", "referenced": False, "tag": 'test_tag', 'notes': 'test notes', 'parents': ['A'], 'position': [0, 0], 'folder_path': 'dummy_folder_path_C'},
            "D": {"status": "staledata", "referenced": False, "tag": 'test_tag', 'notes': 'test notes', 'parents': ['C'], 'position': [0, 0], 'folder_path': 'dummy_folder_path_D'},
            "E": {"status": "ready", "referenced": False, "tag": 'test_tag', 'notes': 'test notes', 'parents': [], 'position': [0, 0,], 'folder_path': 'dummy_folder_path_E'}
        }
    }

@pytest.fixture
def dict_dummy_project():
    # Create a simple dictionary for testing identical to dag_dummy_project
    return {
        "project_id": "pro_12345",
        "notes": "A simple test project DAG",
        "tag": "test_tag",
        "owner": "test_group",
        "nodes": {
            "pip_1": {"tag": "test_tagpip_1", "notes": "test notes", "parents": [], "blocked": False},
            "pip_2": {"tag": "test_tagpip_2", "notes": "test notes", "parents": ["pip_1"], "blocked": False},
            "pip_3": {"tag": "test_tagpip_3", "notes": "test notes", "parents": ["pip_1"], "blocked": False},
            "pip_4": {"tag": "test_tagpip_4", "notes": "test notes", "parents": ["pip_3"], "blocked": False},
            "pip_5": {"tag": "test_tagpip_5", "notes": "test notes", "parents": [], "blocked": True}
        }
    }


PARENT_NODE_LIST =  ["A", "B", "C", "D", "E"]


def dag_detach_1():
    import networkx as nx
    from fusionpipe.utils.pip_utils import NodeState 
    # Create a simple directed acyclic graph (DAG) for testing
    G = nx.DiGraph()
    G.add_edges_from([
        ("A", "B"),
        ("B", "C"),
        ("B", "D"),
    ])
    G.name = "12345"
    G.graph['pipeline_id'] = G.name

    G.nodes['A']['referenced'] = True
    G.nodes['B']['referenced'] = True
    G.nodes['C']['referenced'] = True
    G.nodes['D']['referenced'] = False

    # Add a 'status' attribute to each node using NodeState
    for node in G.nodes:
        G.nodes[node]['position'] = [0, 0]  # Default position
    return G

def dag_detach_2():
    import networkx as nx
    from fusionpipe.utils.pip_utils import NodeState 
    # Create a simple directed acyclic graph (DAG) for testing
    G = nx.DiGraph()
    G.add_edges_from([
        ("A", "B"),
        ("B", "C"),
        ("B", "D"),
        ("E", "D"),
    ])
    G.name = "12345"
    G.graph['pipeline_id'] = G.name

    G.nodes['A']['referenced'] = True
    G.nodes['B']['referenced'] = True
    G.nodes['C']['referenced'] = True
    G.nodes['D']['referenced'] = False
    G.nodes['E']['referenced'] = False

    # Add a 'status' attribute to each node using NodeState
    for node in G.nodes:
        G.nodes[node]['position'] = [0, 0]  # Default position
    return G


def rm_folder(folder_path):
    if os.path.exists(folder_path):
        # Remove all contents but keep the folder itself
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception:
                pass  # Ignore errors    

def remove_files_starting_with(folder_path, prefix):
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            if filename.startswith(prefix):
                file_path = os.path.join(folder_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                except Exception:
                    pass  # Ignore errors


@pytest.fixture(autouse=True)
def cleanup_pytest_carpanes():
    yield
    folder = "/tmp/pytest-of-carpanes"
    rm_folder(folder)

    folder = "/tmp/ray"
    rm_folder(folder)

    remove_files_starting_with('/tmp', "uv-")