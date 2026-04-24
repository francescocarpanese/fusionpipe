"""Pytest fixtures and test environment bootstrap for fusionpipe.

The test suite creates many temporary node folders, and each node can create its
own `.venv`. We intentionally force pytest temporary directories onto the same
mount as `UV_CACHE_DIR` and `FUSIONPIPE_DATA_PATH`.

Why this matters:
- if pytest falls back to a temp directory on a different filesystem than
    `UV_CACHE_DIR`, uv cannot reuse files via hardlinks/reflinks across mounts
    and ends up copying more data into each node-local `.venv`
- keeping both the uv cache and the pytest temp root on the same filesystem
    reduces duplicated disk usage during tests and avoids filling the wrong disk

Environment variables used here:
- `FUSIONPIPE_DATA_PATH`: base folder for fusionpipe data
- `UV_CACHE_DIR`: shared uv cache directory
- `TMPDIR`, `TMP`, `TEMP`: temporary directory root, used for pytest temp data
- `DATABASE_URL_TEST`: PostgreSQL DSN used by the test database fixture
- `USER_UTILS_FOLDER_PATH`: path to the editable helper package used by node init

`developer.env` is loaded here as a fallback for pytest and VS Code test runs so
the suite does not depend on the parent shell having exported these variables.
Already exported variables still take precedence.

Outside pytest, fusionpipe does not require `TMPDIR`, `TMP`, or `TEMP` to be
set. When they are unset, normal runtime code keeps the platform default temp
directory behavior provided by Python and the operating system.
"""

import pytest
import os
import tempfile
import sqlite3

import psycopg2
import random
import string
import re
import shutil

def _load_workspace_env():
    """Load developer.env for test runs when the parent shell did not export it."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "developer.env")
    if not os.path.exists(env_path):
        return

    with open(env_path) as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, os.path.expandvars(value))


def _get_test_temp_root():
    """Resolve a test temp root on the data mount and make tempfile use it."""
    temp_root = os.environ.get("TMPDIR")
    if not temp_root:
        data_path = os.environ.get("FUSIONPIPE_DATA_PATH")
        if data_path:
            temp_root = os.path.join(data_path, "tmp")
        else:
            temp_root = tempfile.gettempdir()

    os.makedirs(temp_root, exist_ok=True)
    os.environ.setdefault("TMPDIR", temp_root)
    os.environ.setdefault("TMP", temp_root)
    os.environ.setdefault("TEMP", temp_root)
    os.environ.setdefault("PYTEST_DEBUG_TEMPROOT", temp_root)
    tempfile.tempdir = temp_root
    return temp_root


_load_workspace_env()
TEST_TEMP_ROOT = _get_test_temp_root()
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
    with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as tmpdir:
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
    G.add_node("A") 
    G.add_node("B")  
    G.add_node("C") 
    G.add_node("D")  
    G.add_node("E")  # Add a node with no edges

    G.add_node("F")  
    G.add_node("G")  
    G.add_node("H")  # This node has 2 parents 

    # Graph with 1 edge for each node
    G.add_edge("A", "B", edge_id="01")
    G.add_edge("A", "C", edge_id="01")
    G.add_edge("C", "D", edge_id="01")

    # Graph with 1 edge for each node
    G.add_edge("F", "H", edge_id="01")
    G.add_edge("G", "H", edge_id="02")

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
        G.nodes[node]['blocked'] = False
        G.nodes[node]['project_id'] = G.graph['project_id']
        if node == "A":
            G.nodes[node]['status'] = "ready"
            G.nodes[node]['blocked'] = True  # A is blocked
            G.nodes[node]['tag'] = 'A'
        elif node == "B":
            G.nodes[node]['status'] = "running"
            G.nodes[node]['tag'] = 'B'
        elif node == "C":
            G.nodes[node]['status'] = "completed"
            G.nodes[node]['tag'] = 'C'
        elif node == "D":
            G.nodes[node]['status'] = "staledata"
            G.nodes[node]['tag'] = 'D'
        elif node == "E":
            G.nodes[node]['status'] = "ready"
            G.nodes[node]['tag'] = 'E'
        elif node == "F":
            G.nodes[node]['status'] = "ready"
            G.nodes[node]['tag'] = 'F'
        elif node == "G":
            G.nodes[node]['status'] = "ready"
            G.nodes[node]['tag'] = 'G'
        elif node == "H":
            G.nodes[node]['status'] = "ready"
            G.nodes[node]['tag'] = 'H'                                    

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
            "A": {"status": "ready", "referenced": False, "tag": 'A', 'notes': 'test notes', 'parents': [], 'parent_edge_ids': {}, 'position': [0, 0], 'folder_path': 'dummy_folder_path_A', 'blocked': True, 'project_id': 'test_project'},
            "B": {"status": "running", "referenced": False, "tag": 'B', 'notes': 'test notes', 'parents': ['A'], 'parent_edge_ids': {'A':'01'}, 'position': [0, 0], 'folder_path': 'dummy_folder_path_B', 'blocked': False, 'project_id': 'test_project'},
            "C": {"status": "completed", "referenced": False, "tag": 'C', 'notes': 'test notes', 'parents': ['A'], 'parent_edge_ids': {'A':'01'}, 'position': [0, 0], 'folder_path': 'dummy_folder_path_C', 'blocked': False, 'project_id': 'test_project'},
            "D": {"status": "staledata", "referenced": False, "tag": 'D', 'notes': 'test notes', 'parents': ['C'], 'parent_edge_ids': {'C':'01'}, 'position': [0, 0], 'folder_path': 'dummy_folder_path_D', 'blocked': False, 'project_id': 'test_project'},
            "E": {"status": "ready", "referenced": False, "tag": 'E', 'notes': 'test notes', 'parents': [], 'parent_edge_ids': {}, 'position': [0, 0,], 'folder_path': 'dummy_folder_path_E', 'blocked': False, 'project_id': 'test_project'},
            "F": {"status": "ready", "referenced": False, "tag": 'F', 'notes': 'test notes', 'parents': [], 'parent_edge_ids': {}, 'position': [0, 0,], 'folder_path': 'dummy_folder_path_F', 'blocked': False, 'project_id': 'test_project'},
            "G": {"status": "ready", "referenced": False, "tag": 'G', 'notes': 'test notes', 'parents': [], 'parent_edge_ids': {}, 'position': [0, 0,], 'folder_path': 'dummy_folder_path_G', 'blocked': False, 'project_id': 'test_project'},
            "H": {"status": "ready", "referenced": False, "tag": 'H', 'notes': 'test notes', 'parents': ['F','G'], 'parent_edge_ids':{'F':'01', 'G':'02'}, 'position': [0, 0,], 'folder_path': 'dummy_folder_path_H', 'blocked': False, 'project_id': 'test_project'}
        }
    }

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
    G.graph['project_id'] = "test_project"

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
    G.graph['project_id'] = "test_project"

    G.nodes['A']['referenced'] = True
    G.nodes['B']['referenced'] = True
    G.nodes['C']['referenced'] = True
    G.nodes['D']['referenced'] = False
    G.nodes['E']['referenced'] = False

    # Add a 'status' attribute to each node using NodeState
    for node in G.nodes:
        G.nodes[node]['position'] = [0, 0]  # Default position
    return G


def rm_subfolders(folder_path):
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

def remove_folder_starting_with(folder_path, prefix):
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            if filename.startswith(prefix):
                file_path = os.path.join(folder_path, filename)
                try:
                    if os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception:
                    pass  # Ignore errors


@pytest.fixture(autouse=True, scope='session')
def cleanup_pytest_carpanes():
    yield
    folder = "/tmp/pytest-of-carpanes"
    rm_subfolders(folder)

    folder = "/tmp/ray"
    rm_subfolders(folder)

    remove_files_starting_with('/tmp', "uv-")
    remove_folder_starting_with('/tmp', "tmp")