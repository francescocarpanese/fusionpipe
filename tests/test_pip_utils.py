import pytest
import tempfile
import os
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
    G.name = "12345"
    G.graph['pipeline_id'] = G.name
    G.graph['notes'] = "A simple test DAG"
    G.graph['tag'] = "test_tag"
    G.graph['owner'] = "test_group"

    # Add a 'status' attribute to each node using NodeState
    for node in G.nodes:
        G.nodes[node]['status'] = NodeState.READY.value
        G.nodes[node]['editable'] = True
        G.nodes[node]['tag'] = 'test_tag'
        G.nodes[node]['notes'] = 'test notes'
    
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
            "A": {"status": "ready", "editable": True, "tag": 'test_tag', 'notes': 'test notes', 'parents': []},
            "B": {"status": "ready", "editable": True, "tag": 'test_tag', 'notes': 'test notes', 'parents': ['A']},
            "C": {"status": "ready", "editable": True, "tag": 'test_tag', 'notes': 'test notes', 'parents': ['A']},
            "D": {"status": "ready", "editable": True, "tag": 'test_tag', 'notes': 'test notes', 'parents': ['C']}
        }
    }


@pytest.fixture
def in_memory_db_conn():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def test_generate_data_folder(tmp_base_dir):
    # Test if the function creates the expected directory structure
    from fusionpipe.utils.pip_utils import generate_data_folder

    base_path = tmp_base_dir
    generate_data_folder(base_path)

    expected_dirs = [
        os.path.join(base_path, "nodes"),
    ]

    for dir_path in expected_dirs:
        assert os.path.exists(dir_path), f"Expected directory {dir_path} does not exist."


def test_init_node_folder(tmp_base_dir):
    # Test if the function creates the expected node folder structure
    from fusionpipe.utils.pip_utils import init_node_folder
    import os

    node_id = "test_node"
    base_path = tmp_base_dir
    init_node_folder(base_path, node_id, verbose=True)

    node_folder = os.path.join(base_path, node_id)
    code_folder = os.path.join(node_folder, "code")
    data_folder = os.path.join(node_folder, "data")
    reports_folder = os.path.join(node_folder, "reports")

    assert os.path.isdir(node_folder), "Node folder was not created."
    assert os.path.isdir(code_folder), "Code subfolder was not created."
    assert os.path.isdir(data_folder), "Data subfolder was not created."
    assert os.path.isdir(reports_folder), "Reports subfolder was not created."


def test_delete_node_folder_removes_existing_folder(tmp_base_dir):
    # Test if the function deletes an existing node folder
    from fusionpipe.utils.pip_utils import delete_node_folder
    import os

    node_id = "testnode"
    node_folder_path = os.path.join(tmp_base_dir, f"n_{node_id}")
    os.makedirs(node_folder_path, exist_ok=True)
    # Ensure the folder exists before deletion
    assert os.path.exists(node_folder_path)
    delete_node_folder(tmp_base_dir, node_id, verbose=True)
    # Folder should be deleted
    assert not os.path.exists(node_folder_path)

def test_delete_node_folder_nonexistent_folder(tmp_base_dir, capsys):
    # Test if the function handles non-existent node folders gracefully
    from fusionpipe.utils.pip_utils import delete_node_folder
    import os
    node_id = "nonexistentnode"
    node_folder_path = os.path.join(tmp_base_dir, f"n_{node_id}")
    # Ensure the folder does not exist
    if os.path.exists(node_folder_path):
        os.rmdir(node_folder_path)
    delete_node_folder(tmp_base_dir, node_id, verbose=True)
    # Should not raise, and should print a message
    captured = capsys.readouterr()
    assert f"Node folder does not exist" in captured.out


def test_create_db(tmp_database_path):
    from fusionpipe.utils import db_utils
    db_file_path = tmp_database_path
    db_utils.create_db(db_file_path)
    # Check if the database file was created
    assert os.path.exists(db_file_path), f"Connection database {db_file_path} was not created."


def test_graph_to_dict(dag_dummy_1, dict_dummy_1):
    from fusionpipe.utils.pip_utils import graph_to_dict
    import networkx as nx

    # Convert the graph to a dictionary
    graph_dict = graph_to_dict(dag_dummy_1)

    # Check if the converted dictionary matches the expected dictionary
    assert graph_dict == dict_dummy_1, "Graph to dict conversion did not produce the expected result."



# To be changes

def test_graph_to_db(in_memory_db_conn, dag_graph_dummy_1):
    import networkx as nx
    from fusionpipe.utils.pip_utils import graph_to_db
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    # Setup database
    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    G = dag_graph_dummy_1

    # Call the function
    graph_to_db(G, cur)
    conn.commit()

    # Check pipeline exists
    cur.execute("SELECT * FROM pipelines WHERE pipeline_id=?", (G.graph['id'],))
    pipeline_row = cur.fetchone()
    assert pipeline_row is not None, "Pipeline was not added to the database."
    assert pipeline_row[1] == G.graph['tag'], "Pipeline tag does not match expected value."

    # Check nodes exist
    for node in G.nodes:
        cur.execute("SELECT * FROM nodes WHERE node_id=?", (node,))
        node_row = cur.fetchone()
        assert node_row is not None, f"Node {node} was not added to the database."
        assert node_row[1] == G.nodes[node]['status'], f"Node {node} status does not match expected value."

    # Check edges exist
    for parent, child in G.edges:
        cur.execute("SELECT * FROM node_relation WHERE parent_id=? AND child_id=?", (parent, child))
        relation_row = cur.fetchone()
        assert relation_row is not None, f"Relation between {parent} and {child} was not added to the database."


def test_db_to_graph(in_memory_db_conn, dag_graph_dummy_1):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import graph_to_db, db_to_graph_from_pip_id
    import networkx as nx

    # Setup database
    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    graph_to_db(dag_graph_dummy_1, cur)
    conn.commit()

    # Call the function to convert DB back to graph
    G_retrieved = db_to_graph_from_pip_id(cur, dag_graph_dummy_1.graph['id'])

    # Check if the retrieved graph matches the original
    assert nx.is_isomorphic(G_retrieved, dag_graph_dummy_1), "Retrieved graph does not match the original graph."

    conn.close()
