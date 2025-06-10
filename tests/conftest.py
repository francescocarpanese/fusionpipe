import pytest
import os
import tempfile
import sqlite3


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
    G.graph['notes'] = "A simple test DAG"
    G.graph['tag'] = "test_tag"
    G.graph['owner'] = "test_group"

    # Add a 'status' attribute to each node using NodeState
    for node in G.nodes:
        G.nodes[node]['editable'] = True
        G.nodes[node]['tag'] = 'test_tag'
        G.nodes[node]['folder_path'] = 'dummy_folder_path'
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
            G.nodes[node]['status'] = "staledata"            
    return G

@pytest.fixture
def dict_dummy_1():
    # Create a simple dictionary for testing identical to dag_dummy_1
    return {
        "pipeline_id": "12345",
        "notes": "A simple test DAG",
        "tag": "test_tag",
        "owner": "test_group",
        "nodes": {
            "A": {"status": "ready", "editable": True, "tag": 'test_tag', 'notes': 'test notes', 'parents': [], 'position': [0, 0], 'folder_path': 'dummy_folder_path'},
            "B": {"status": "running", "editable": True, "tag": 'test_tag', 'notes': 'test notes', 'parents': ['A'], 'position': [0, 0], 'folder_path': 'dummy_folder_path'},
            "C": {"status": "completed", "editable": True, "tag": 'test_tag', 'notes': 'test notes', 'parents': ['A'], 'position': [0, 0], 'folder_path': 'dummy_folder_path'},
            "D": {"status": "staledata", "editable": True, "tag": 'test_tag', 'notes': 'test notes', 'parents': ['C'], 'position': [0, 0], 'folder_path': 'dummy_folder_path'},
            "E": {"status": "staledata", "editable": True, "tag": 'test_tag', 'notes': 'test notes', 'parents': [], 'position': [0, 0,], 'folder_path': 'dummy_folder_path'}
        }
    }

@pytest.fixture
def in_memory_db_conn():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()

PARENT_NODE_LIST =  ["A", "B", "C", "D", "E"]