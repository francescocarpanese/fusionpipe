import pytest
import tempfile
import os

# Define useful fixtures

# @pytest.fixture
# def tmp_base_dir():
#     with tempfile.TemporaryDirectory() as tmpdir:
#         yield tmpdir

# @pytest.fixture
# def pip_settings(tmp_base_dir):
#     return {
#         "pipeline_folder": os.path.join(tmp_base_dir, "pipelines"),
#         "node_folder": os.path.join(tmp_base_dir, "nodes"),
#     }


@pytest.fixture
def tmp_base_dir():
    yield "/misc/carpanes/fusionpipe/bin"

@pytest.fixture
def pip_settings(tmp_base_dir):
    return {
        "connection_db_filepath": "/misc/carpanes/fusionpipe/bin/connection.db",
        "node_folder": "/misc/carpanes/fusionpipe/bin/nodes",
    }

@pytest.fixture
def dag_graph_dummy_1():
    import networkx as nx
    # Create a simple directed acyclic graph (DAG) for testing
    G = nx.DiGraph()
    G.add_edges_from([
        ("A", "B"),
        ("A", "C"),
        ("C", "D"),
    ])
    G.name = "test_dag"
    G.graph['description'] = "A simple test DAG"
    G.graph['id'] = "12345"
    return G

@pytest.fixture
def dict_graph_dummy_1():
    return  {
        "name": "test_dag",
        "nodes": [
            {"nid": "A", "parents": []},
            {"nid": "B", "parents": ["A"]},
            {"nid": "C", "parents": ["A"]},
            {"nid": "D", "parents": ["C"]}
        ]
    }


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



def test_create_node_folder(pip_settings):
    from fusionpipe.utils.pip_utils import init_node_folder, generate_node_id

    node_id = generate_node_id()
    settings = pip_settings.copy()

    init_node_folder(settings, node_id=node_id, verbose=False)
    
    # Check if the node folder was created
    node_folder_path = os.path.join(settings["node_folder"], node_id)
    assert os.path.exists(node_folder_path), f"Node folder {node_folder_path} was not created."
    

def test_generate_connection_db(pip_settings):
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    pipeline_db.init_db(db_path)
    # Check if the database file was created
    assert os.path.exists(db_path), f"Connection database {db_path} was not created."


def test_add_pipeline(pip_settings):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import generate_pip_id

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    pip_id = generate_pip_id()
    pipeline_db.add_pipeline(cur, pipeline_id=pip_id, tag="test_pipeline")
    
    # Commit and close the connection
    conn.commit()
    conn.close()

    # Check if the pipeline was added
    conn = pipeline_db.load_db(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM pipelines WHERE id=?", (pip_id,))
    result = cur.fetchone()
    
    assert result is not None, f"Pipeline {pip_id} was not added to the database."
    assert result[1] == "test_pipeline", "Pipeline tag does not match expected value."

def test_add_node(pip_settings):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    node_id = generate_node_id()
    pipeline_db.add_node(cur, node_id=node_id)
    
    # Commit and close the connection
    conn.commit()
    conn.close()

    # Check if the node was added
    conn = pipeline_db.load_db(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM nodes WHERE id=?", (node_id,))
    result = cur.fetchone()
    
    assert result is not None, f"Node {node_id} was not added to the database."

def test_add_node_to_entries(pip_settings):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    pipeline_db.add_node(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add entry to entries table
    user = "test_user"
    entry_id = pipeline_db.add_node_to_entries(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()
    conn.close()

    # Check if the entry was added
    conn = pipeline_db.load_db(db_path)
    cur = conn.cursor()
    cur.execute("SELECT node_id, pipeline_id, user FROM entries WHERE id=?", (entry_id,))
    result = cur.fetchone()
    assert result is not None, "Entry was not added to the database."
    assert result[0] == node_id, "Node ID in entry does not match."
    assert result[1] == pipeline_id, "Pipeline ID in entry does not match."
    assert result[2] == user, "User in entry does not match."


def test_remove_node_from_entries(pip_settings):
    from fusionpipe.utils.pipeline_db import remove_node_from_entries
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db


    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    pipeline_db.add_node(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add entry to entries table
    user = "test_user"
    entry_id = pipeline_db.add_node_to_entries(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Remove the entry
    rows_deleted = remove_node_from_entries(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()
    assert rows_deleted == 1, "Entry was not deleted from the database."

    # Check if the entry was actually removed
    cur.execute("SELECT * FROM entries WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    result = cur.fetchone()
    conn.close()
    assert result is None, "Entry was not removed from the database."


def test_add_node_relation(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import pipeline_db
    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create two nodes
    child_id = generate_node_id()
    parent_id = generate_node_id()
    pipeline_db.add_node(cur, node_id=child_id)
    pipeline_db.add_node(cur, node_id=parent_id)

    # Add relation
    relation_id = pipeline_db.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()

    # Check that the relation exists
    cur.execute("SELECT child_id, parent_id FROM node_relation WHERE id=?", (relation_id,))
    result = cur.fetchone()
    conn.close()
    assert result is not None, "Node relation was not added to the database."
    assert result[0] == child_id, "Child ID in node relation does not match."
    assert result[1] == parent_id, "Parent ID in node relation does not match."

def test_get_node_parents(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create 3 nodes
    node1 = generate_node_id()
    node2 = generate_node_id()
    node3 = generate_node_id()
    pipeline_db.add_node(cur, node_id=node1)
    pipeline_db.add_node(cur, node_id=node2)
    pipeline_db.add_node(cur, node_id=node3)

    # Add relation: node1 is parent of node2
    pipeline_db.add_node_relation(cur, child_id=node2, parent_id=node1)
    conn.commit()

    # node2 should have node1 as parent
    parents_node2 = pipeline_db.get_node_parents(cur, node2)
    assert parents_node2 == [node1], f"Expected parent of node2 to be [{node1}], got {parents_node2}"

    # node1 should have no parents
    parents_node1 = pipeline_db.get_node_parents(cur, node1)
    assert parents_node1 == [], f"Expected no parents for node1, got {parents_node1}"

    # node3 should have no parents
    parents_node3 = pipeline_db.get_node_parents(cur, node3)
    assert parents_node3 == [], f"Expected no parents for node3, got {parents_node3}"

    conn.close()


def test_get_node_children(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import pipeline_db


    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create 3 nodes
    node1 = generate_node_id()
    node2 = generate_node_id()
    node3 = generate_node_id()
    pipeline_db.add_node(cur, node_id=node1)
    pipeline_db.add_node(cur, node_id=node2)
    pipeline_db.add_node(cur, node_id=node3)

    # Add relation: node1 is parent of node2
    pipeline_db.add_node_relation(cur, child_id=node2, parent_id=node1)
    # Add relation: node1 is parent of node3
    pipeline_db.add_node_relation(cur, child_id=node3, parent_id=node1)
    conn.commit()

    # node1 should have node2 and node3 as children
    children_node1 = pipeline_db.get_node_children(cur, node1)
    assert set(children_node1) == set([node2, node3]), f"Expected children of node1 to be [{node2}, {node3}], got {children_node1}"

    # node2 should have no children
    children_node2 = pipeline_db.get_node_children(cur, node2)
    assert children_node2 == [], f"Expected no children for node2, got {children_node2}"

    # node3 should have no children
    children_node3 = pipeline_db.get_node_children(cur, node3)
    assert children_node3 == [], f"Expected no children for node3, got {children_node3}"


def test_add_pipeline_description(pip_settings):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import generate_pip_id

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Add a pipeline to reference
    pipeline_id = generate_pip_id()
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    conn.commit()

    # Add a description to the pipeline
    desc1 = "This is a test pipeline description."
    rowid1 = pipeline_db.add_pipeline_description(cur, pipeline_id, desc1)
    conn.commit()
    assert rowid1 is not None, "Failed to add pipeline description."
    assert isinstance(rowid1, int), "Row ID should be an integer."
    assert rowid1 > 0, "Row ID should be greater than zero."