import pytest
import tempfile
import os
import sqlite3

# Define useful fixtures

local = False

if not local:
    @pytest.fixture
    def tmp_base_dir():
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def pip_settings(tmp_base_dir):
        return {
            "connection_db_filepath": os.path.join(tmp_base_dir, "connection.db"),
            "node_folder": os.path.join(tmp_base_dir, "nodes"),
        }
    
    @pytest.fixture
    def tmp_database_path(tmp_base_dir):
        return os.path.join(tmp_base_dir, "connection.db")

    @pytest.fixture
    def tmp_database_path(tmp_base_dir):
        db_path  = os.path.join(tmp_base_dir, "connection.db")
        yield db_path
        if os.path.exists(db_path):  # Cleanup after the test
            os.remove(db_path)       

else:
    @pytest.fixture
    def tmp_base_dir():
        yield "/home/cisko90/fusionpipe/bin"

    @pytest.fixture
    def pip_settings(tmp_base_dir):
        return {
            "connection_db_filepath": f"{tmp_base_dir}/connection.db",
            "node_folder": "f{tmp_base_dir}/nodes",
        }
    
    @pytest.fixture
    def tmp_database_path(tmp_base_dir):
        # This does not remove the database so that it can be inspected
        db_path  = os.path.join(tmp_base_dir, "connection.db")
        yield db_path
        if os.path.exists(db_path):  # Cleanup after the test
            os.remove(db_path)   

@pytest.fixture
def dag_graph_dummy_1():
    import networkx as nx
    from fusionpipe.utils.pip_utils import NodeState 
    # Create a simple directed acyclic graph (DAG) for testing
    G = nx.DiGraph()
    G.add_edges_from([
        ("A", "B"),
        ("A", "C"),
        ("C", "D"),
    ])
    G.graph['description'] = "A simple test DAG"
    G.graph['id'] = "12345"
    G.graph['tag'] = "test_tag"

    # Add a 'status' attribute to each node using NodeState
    for node in G.nodes:
        G.nodes[node]['status'] = NodeState.READY.value

    return G


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
    cur.execute("SELECT * FROM pipelines WHERE pipeline_id=?", (pip_id,))
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
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    
    # Commit and close the connection
    conn.commit()
    conn.close()

    # Check if the node was added
    conn = pipeline_db.load_db(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM nodes WHERE node_id=?", (node_id,))
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
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add entry to node_pipeline_relation table
    user = "test_user"
    entry_id = pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()
    conn.close()

    # Check if the entry was added
    conn = pipeline_db.load_db(db_path)
    cur = conn.cursor()
    cur.execute("SELECT node_id, pipeline_id, user FROM node_pipeline_relation WHERE id=?", (entry_id,))
    result = cur.fetchone()
    assert result is not None, "Entry was not added to the database."
    assert result[0] == node_id, "Node ID in entry does not match."
    assert result[1] == pipeline_id, "Pipeline ID in entry does not match."
    assert result[2] == user, "User in entry does not match."


def test_remove_node_from_pipeline(pip_settings):
    from fusionpipe.utils.pipeline_db import remove_node_from_pipeline
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db


    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    # Add a tag to the node
    tag = "test_tag"
    pipeline_db.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag=tag)

    # Add entry to node_pipeline_relation table
    user = "test_user"
    entry_id = pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Remove the entry
    cur = conn.cursor()
    rows_deleted = remove_node_from_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()
    assert rows_deleted == 2, "Entry was not deleted from the database."

    # Check if the entry was actually removed
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
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
    pipeline_db.add_node_to_nodes(cur, node_id=child_id)
    pipeline_db.add_node_to_nodes(cur, node_id=parent_id)

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
    pipeline_db.add_node_to_nodes(cur, node_id=node1)
    pipeline_db.add_node_to_nodes(cur, node_id=node2)
    pipeline_db.add_node_to_nodes(cur, node_id=node3)

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
    pipeline_db.add_node_to_nodes(cur, node_id=node1)
    pipeline_db.add_node_to_nodes(cur, node_id=node2)
    pipeline_db.add_node_to_nodes(cur, node_id=node3)

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


def test_update_node_status(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, NodeState
    from fusionpipe.utils import pipeline_db


    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node
    node_id = generate_node_id()
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    conn.commit()

    # Update node status to RUNNING
    pipeline_db.update_node_status(cur, node_id=node_id, status=NodeState.RUNNING.value)
    conn.commit()

    # Check if the status was updated
    cur.execute("SELECT status FROM nodes WHERE node_id=?", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found in database."
    assert result[0] == NodeState.RUNNING.value, f"Expected status {NodeState.RUNNING.value}, got {result[0]}"

    conn.close()



def test_get_pipeline_tag(pip_settings):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import generate_pip_id

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Add a pipeline with a specific tag
    pipeline_id = generate_pip_id()
    tag = "my_test_tag"
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Test get_pipeline_tag returns the correct tag
    fetched_tag = pipeline_db.get_pipeline_tag(cur, pipeline_id)
    assert fetched_tag == tag, f"Expected tag '{tag}', got '{fetched_tag}'"

    # Test get_pipeline_tag returns None for non-existent pipeline
    non_existent_id = "nonexistent_id"
    assert pipeline_db.get_pipeline_tag(cur, non_existent_id) is None

    conn.close()



def test_get_all_nodes_from_pip_id(pip_settings):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a pipeline and nodes
    pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(3)]
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    for node_id in node_ids:
        pipeline_db.add_node_to_nodes(cur, node_id=node_id)
        pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
    conn.commit()


    result_nodes = pipeline_db.get_all_nodes_from_pip_id(cur, pipeline_id)
    assert set(result_nodes) == set(node_ids), f"Expected nodes {node_ids}, got {result_nodes}"

    # Test with a pipeline that has no nodes
    empty_pipeline_id = generate_pip_id()
    pipeline_db.add_pipeline(cur, pipeline_id=empty_pipeline_id, tag="empty_pipeline")
    conn.commit()
    result_empty = pipeline_db.get_all_nodes_from_pip_id(cur, empty_pipeline_id)
    assert result_empty == [], "Expected empty list for pipeline with no nodes"

    conn.close()



def test_graph_to_db(pip_settings, dag_graph_dummy_1):
    import networkx as nx
    from fusionpipe.utils.pip_utils import graph_to_db
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    # Setup database
    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

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


def test_db_to_graph(pip_settings, dag_graph_dummy_1):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import graph_to_db, db_to_graph_from_pip_id
    import networkx as nx

    # Setup database
    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Add the dummy graph to the database
    graph_to_db(dag_graph_dummy_1, cur)
    conn.commit()

    # Call the function to convert DB back to graph
    G_retrieved = db_to_graph_from_pip_id(cur, dag_graph_dummy_1.graph['id'])

    # Check if the retrieved graph matches the original
    assert nx.is_isomorphic(G_retrieved, dag_graph_dummy_1), "Retrieved graph does not match the original graph."

    conn.close()


def test_get_nodes_without_pipeline(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create nodes
    node_ids = [generate_node_id() for _ in range(3)]
    for node_id in node_ids:
        pipeline_db.add_node_to_nodes(cur, node_id=node_id)

    # Create a pipeline and associate one node with it
    pipeline_id = generate_pip_id()
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    pipeline_db.add_node_to_pipeline(cur, node_id=node_ids[0], pipeline_id=pipeline_id, user="test_user")
    conn.commit()

    # Get nodes without a pipeline
    nodes_without_pipeline = pipeline_db.get_nodes_without_pipeline(cur)
    conn.close()

    # Check that only the nodes not associated with a pipeline are returned
    expected_nodes = set(node_ids[1:])
    assert set(nodes_without_pipeline) == expected_nodes, f"Expected nodes without pipeline: {expected_nodes}, got: {nodes_without_pipeline}"

def test_remove_node_from_tags(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db


    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add a tag to the node
    tag = "test_tag"
    pipeline_db.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Remove the tag
    rows_deleted = pipeline_db.remove_node_from_tags(cur, node_id=node_id)
    conn.commit()
    assert rows_deleted == 1, "Tag was not removed from the database."

    # Check if the tag was actually removed
    cur.execute("SELECT * FROM node_tags WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    result = cur.fetchone()
    conn.close()
    assert result is None, "Tag was not removed from the database."


def test_remove_node_from_relations(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import pipeline_db


    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create nodes
    parent_id = generate_node_id()
    child_id = generate_node_id()
    pipeline_db.add_node_to_nodes(cur, node_id=parent_id)
    pipeline_db.add_node_to_nodes(cur, node_id=child_id)

    # Add relation
    pipeline_db.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()

    # Remove the relation
    rows_deleted = pipeline_db.remove_node_from_relations(cur, node_id=child_id)
    conn.commit()
    assert rows_deleted == 1, "Relation was not removed from the database."

    # Check if the relation was actually removed
    cur.execute("SELECT * FROM node_relation WHERE child_id=? OR parent_id=?", (child_id, child_id))
    result = cur.fetchone()
    conn.close()
    assert result is None, "Relation was not removed from the database."

def test_remove_node_from_everywhere(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add the node to node_pipeline_relation
    pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")

    # Add a tag to the node
    tag = "test_tag"
    pipeline_db.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag=tag)

    # Add a relation involving the node
    parent_id = generate_node_id()
    pipeline_db.add_node_to_nodes(cur, node_id=parent_id)
    pipeline_db.add_node_relation(cur, child_id=node_id, parent_id=parent_id)
    conn.commit()

    # Remove the node from everywhere
    pipeline_db.remove_node_from_everywhere(cur, node_id=node_id)
    conn.commit()
    cur = conn.cursor()

    # Verify the node is removed from all relevant tables
    assert pipeline_db.get_rows_with_node_id_in_entries(cur, node_id) == [], "Node was not removed from node_pipeline_relation."
    assert pipeline_db.get_rows_node_id_in_nodes(cur, node_id) == [], "Node was not removed from nodes."
    assert pipeline_db.get_rows_with_node_id_in_node_tags(cur, node_id) == [], "Node was not removed from tags."
    assert pipeline_db.get_rows_with_node_id_relations(cur, node_id) == [], "Node was not removed from relations."

    conn.close()

def test_remove_node_from_entries(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add the node to node_pipeline_relation
    user = "test_user"
    entry_id = pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Remove the node from node_pipeline_relation
    rows_deleted = pipeline_db.remove_node_from_node_pipeline_relation(cur, node_id=node_id)
    conn.commit()
    assert rows_deleted == 1, "Entry was not removed from the database."

    # Verify the entry is removed
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    result = cur.fetchone()
    conn.close()
    assert result is None, "Entry was not removed from the database."


def test_get_rows_with_node_id_in_entries(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add the node to node_pipeline_relation
    user = "test_user"
    entry_id = pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Get rows with the node ID in node_pipeline_relation
    rows = pipeline_db.get_rows_with_node_id_in_entries(cur, node_id)
    assert len(rows) == 1, "Expected one entry with the node ID."
    assert rows[0][3] == node_id, "Node ID in entry does not match expected value."
    assert rows[0][4] == pipeline_id, "Pipeline ID in entry does not match expected value."
    
    conn.close()


def test_get_rows_node_id_in_nodes(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node
    node_id = generate_node_id()
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    conn.commit()

    # Get rows with the node ID in nodes
    rows = pipeline_db.get_rows_node_id_in_nodes(cur, node_id)
    assert len(rows) == 1, "Expected one row with the node ID."
    assert rows[0][0] == node_id, "Node ID in row does not match expected value."

    conn.close()

def test_get_rows_with_node_id_in_node_tags(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add a tag to the node
    tag = "test_tag"
    pipeline_db.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Get rows with the node ID in tags
    rows = pipeline_db.get_rows_with_node_id_in_node_tags(cur, node_id)
    assert len(rows) == 1, "Expected one row with the node ID in tags."

    conn.close()


def test_get_rows_with_node_id_relations(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create two nodes and a relation
    parent_id = generate_node_id()
    child_id = generate_node_id()
    pipeline_db.add_node_to_nodes(cur, node_id=parent_id)
    pipeline_db.add_node_to_nodes(cur, node_id=child_id)
    pipeline_db.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()

    # Get rows with the child node ID in relations
    rows = pipeline_db.get_rows_with_node_id_relations(cur, child_id)

    conn.close()
    assert len(rows) == 1, "Expected one row with the child node ID in relations."

def test_get_rows_with_pipeline_id_in_entries(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a pipeline and nodes
    pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(2)]
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    for node_id in node_ids:
        pipeline_db.add_node_to_nodes(cur, node_id=node_id)
        pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
    conn.commit()

    # Get rows with the pipeline ID in node_pipeline_relation
    rows = pipeline_db.get_rows_with_pipeline_id_in_entries(cur, pipeline_id)
    conn.close()

    # Verify the rows match the expected data
    assert len(rows) == len(node_ids), f"Expected {len(node_ids)} rows, got {len(rows)}."
    for row in rows:
        assert row[4] == pipeline_id, f"Pipeline ID in row does not match expected value {pipeline_id}."
        assert row[3] in node_ids, f"Node ID {row[3]} in row is not in the expected node IDs {node_ids}."


def test_get_rows_with_pipeline_id_in_pipelines(pip_settings):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import pipeline_db


    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a pipeline
    pipeline_id = generate_pip_id()
    tag = "test_pipeline"
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Get rows with the pipeline ID in pipelines
    rows = pipeline_db.get_rows_with_pipeline_id_in_pipelines(cur, pipeline_id)
    conn.close()

    # Verify the rows match the expected data
    assert len(rows) == 1, f"Expected one row, got {len(rows)}."
    assert rows[0][0] == pipeline_id, f"Pipeline ID in row does not match expected value {pipeline_id}."
    assert rows[0][1] == tag, f"Pipeline tag in row does not match expected value {tag}."



def test_duplicate_pipeline(pip_settings):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pipeline_db import add_pipeline, add_pipeline_description, add_node_to_pipeline, add_node_tag, duplicate_pipeline

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Add a source pipeline
    source_pipeline_id = "source_pipeline"
    add_pipeline(cur, pipeline_id=source_pipeline_id, tag="v1.0")
    add_pipeline_description(cur, pipeline_id=source_pipeline_id, description="Test pipeline description")

    # Add nodes and node_pipeline_relation to the source pipeline
    add_node_to_pipeline(cur, node_id="node1", pipeline_id=source_pipeline_id, user="user1")
    add_node_to_pipeline(cur, node_id="node2", pipeline_id=source_pipeline_id, user="user2")
    add_node_tag(cur, node_id="node1", pipeline_id=source_pipeline_id, tag="tag1")
    add_node_tag(cur, node_id="node2", pipeline_id=source_pipeline_id, tag="tag2")

    # Commit changes
    conn.commit()

    # Duplicate the pipeline
    new_pipeline_id = "new_pipeline"
    duplicate_pipeline(cur, source_pipeline_id, new_pipeline_id)

    # Verify the new pipeline exists in the pipelines table
    cur.execute("SELECT * FROM pipelines WHERE pipeline_id = ?", (new_pipeline_id,))
    new_pipeline = cur.fetchone()
    assert new_pipeline is not None, "New pipeline was not created."
    assert new_pipeline[0] == new_pipeline_id
    assert new_pipeline[1] == "v1.0"  # Tag should match the source pipeline

    # Verify the node_pipeline_relation table
    cur.execute("SELECT * FROM node_pipeline_relation WHERE pipeline_id = ?", (new_pipeline_id,))
    node_pipeline_relation = cur.fetchall()
    assert len(node_pipeline_relation) == 2, "Entries were not duplicated correctly."
    assert node_pipeline_relation[0][3] == "node1"
    assert node_pipeline_relation[1][3] == "node2"

    # Verify the node_tags table
    cur.execute("SELECT * FROM node_tags WHERE pipeline_id = ?", (new_pipeline_id,))
    node_tags = cur.fetchall()
    assert len(node_tags) == 2, "Node tags were not duplicated correctly."
    assert node_tags[0][1] == "tag1"
    assert node_tags[1][1] == "tag2"

    # Verify the pipeline_description table
    cur.execute("SELECT * FROM pipeline_description WHERE pipeline_id = ?", (new_pipeline_id,))
    description = cur.fetchone()
    assert description is not None, "Pipeline description was not duplicated."
    assert description[1] == "Test pipeline description"

    print("All assertions passed for test_duplicate_pipeline.")


def test_duplicate_pipeline_graph_comparison(pip_settings, dag_graph_dummy_1):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import graph_to_db, db_to_graph_from_pip_id
    import networkx as nx

    # Setup database
    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Add the original graph to the database
    original_graph = dag_graph_dummy_1
    graph_to_db(original_graph, cur)
    conn.commit()

    # Duplicate the pipeline
    original_pipeline_id = original_graph.graph['id']
    new_pipeline_id = "duplicated_pipeline"
    pipeline_db.duplicate_pipeline(cur, original_pipeline_id, new_pipeline_id)
    conn.commit()

    # Load the original and duplicated pipelines as graphs
    original_graph_loaded = db_to_graph_from_pip_id(cur, original_pipeline_id)
    duplicated_graph_loaded = db_to_graph_from_pip_id(cur, new_pipeline_id)

    # Compare the graphs
    assert nx.is_isomorphic(
        original_graph_loaded, duplicated_graph_loaded,
        node_match=lambda n1, n2: n1['status'] == n2['status']
    ), "Duplicated graph does not match the original graph structure and attributes."

    # Ensure the pipeline IDs are different
    assert original_graph_loaded.graph['id'] != duplicated_graph_loaded.graph['id'], \
        "Pipeline IDs should be different between the original and duplicated graphs."

    conn.close()

def test_dupicate_node_in_pipeline_full_coverage(pip_settings):
    from fusionpipe.utils import pipeline_db
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create original node and another node (for relation)
    original_node_id = generate_node_id()
    other_node_id = generate_node_id()
    pipeline_db.add_node_to_nodes(cur, node_id=original_node_id)
    pipeline_db.add_node_to_nodes(cur, node_id=other_node_id)

    # Add a relation: original_node_id is child, other_node_id is parent
    pipeline_db.add_node_relation(cur, child_id=original_node_id, parent_id=other_node_id)
    # Add a relation: original_node_id is parent, other_node_id is child
    pipeline_db.add_node_relation(cur, child_id=other_node_id, parent_id=original_node_id)

    # Create a pipeline and add an entry connecting the pipeline to the node
    pipeline_id = generate_pip_id()
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    user = "test_user"
    pipeline_db.add_node_to_pipeline(cur, node_id=original_node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Add a tag to the original node
    tag = "test_tag"
    pipeline_db.add_node_tag(cur, node_id=original_node_id, pipeline_id=pipeline_id, tag=tag)

    # Duplicate the node
    duplicated_node_id = f"{original_node_id}_copy"
    pipeline_db.dupicate_node_in_pipeline(cur, original_node_id, duplicated_node_id, pipeline_id)
    conn.commit()

    # Check nodes table
    cur.execute("SELECT * FROM nodes WHERE node_id = ?", (duplicated_node_id,))
    duplicated_node = cur.fetchone()
    assert duplicated_node is not None, "Duplicated node was not created."

    # Check node_tags table
    cur.execute("SELECT * FROM node_tags WHERE node_id = ? AND pipeline_id = ?", (duplicated_node_id, pipeline_id,))
    duplicated_tag = cur.fetchone()
    assert duplicated_tag is not None, "Duplicated node tag was not created."
    assert duplicated_tag[1] == tag, "Duplicated node tag does not match the original node's tag."

    # Check node_pipeline_relation table
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id = ? AND pipeline_id = ?", (duplicated_node_id, pipeline_id,))
    duplicated_entry = cur.fetchone()
    assert duplicated_entry is not None, "Duplicated node entry was not created."
    assert duplicated_entry[4] == pipeline_id, "Duplicated entry pipeline_id does not match original."


    conn.close()



def test_copy_node_relations(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create nodes
    source_node_id = generate_node_id()
    parent1_id = generate_node_id()
    parent2_id = generate_node_id()
    child1_id = generate_node_id()
    child2_id = generate_node_id()
    new_node_id = generate_node_id()

    # Add nodes to the database
    for node_id in [source_node_id, parent1_id, parent2_id, child1_id, child2_id, new_node_id]:
        pipeline_db.add_node_to_nodes(cur, node_id=node_id)

    # Add parent relations (source_node_id is child of parent1 and parent2)
    pipeline_db.add_node_relation(cur, child_id=source_node_id, parent_id=parent1_id)
    pipeline_db.add_node_relation(cur, child_id=source_node_id, parent_id=parent2_id)

    # Add child relations (child1 and child2 are children of source_node_id)
    pipeline_db.add_node_relation(cur, child_id=child1_id, parent_id=source_node_id)
    pipeline_db.add_node_relation(cur, child_id=child2_id, parent_id=source_node_id)
    conn.commit()

    # Copy relations from source_node_id to new_node_id
    pipeline_db.copy_node_relations(cur, source_node_id, new_node_id)
    conn.commit()

    # Check parent relations for new_node_id (should match source_node_id's parents)
    cur.execute("SELECT parent_id FROM node_relation WHERE child_id=?", (new_node_id,))
    parent_rows = cur.fetchall()
    parent_ids = {row[0] for row in parent_rows}
    assert parent1_id in parent_ids and parent2_id in parent_ids, \
        f"Parent relations not copied correctly: {parent_ids}"

    # Check child relations for new_node_id (should match source_node_id's children)
    cur.execute("SELECT child_id FROM node_relation WHERE parent_id=?", (new_node_id,))
    child_rows = cur.fetchall()
    child_ids = {row[0] for row in child_rows}
    assert child1_id in child_ids and child2_id in child_ids, \
        f"Child relations not copied correctly: {child_ids}"

    conn.close()

def test_duplicate_node_in_pipeline_with_relations(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import pipeline_db


    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create nodes
    source_node_id = generate_node_id()
    parent_id = generate_node_id()
    child_id = generate_node_id()
    new_node_id = f"{source_node_id}_copy"

    # Add nodes to the database
    for node_id in [source_node_id, parent_id, child_id]:
        pipeline_db.add_node_to_nodes(cur, node_id=node_id)

    # Add parent and child relations for the source node
    pipeline_db.add_node_relation(cur, child_id=source_node_id, parent_id=parent_id)
    pipeline_db.add_node_relation(cur, child_id=child_id, parent_id=source_node_id)
    conn.commit()

    # Create a pipeline and add an entry connecting the pipeline to the source node
    pipeline_id = generate_node_id()
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Duplicate node with relations
    pipeline_db.duplicate_node_in_pipeline_with_relations(cur, source_node_id, new_node_id, pipeline_id)
    conn.commit()

    # Check that the new node exists
    cur.execute("SELECT * FROM nodes WHERE node_id = ?", (new_node_id,))
    assert cur.fetchone() is not None, "Duplicated node was not created."

    # Check parent relations for new node
    cur.execute("SELECT parent_id FROM node_relation WHERE child_id = ?", (new_node_id,))
    parent_rows = cur.fetchall()
    parent_ids = {row[0] for row in parent_rows}
    assert parent_id in parent_ids, "Parent relation was not copied to duplicated node."

    # Check child relations for new node
    cur.execute("SELECT child_id FROM node_relation WHERE parent_id = ?", (new_node_id,))
    child_rows = cur.fetchall()
    child_ids = {row[0] for row in child_rows}
    assert child_id in child_ids, "Child relation was not copied to duplicated node."

    conn.close()


def test_remove_node_from_pipeline_removes_tags_and_entries(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db


    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Setup: create node, pipeline, entry, and tag
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
    pipeline_db.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag="test_tag")
    conn.commit()

    # Remove node from pipeline
    rows_deleted = pipeline_db.remove_node_from_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Check that the entry is removed from node_pipeline_relation
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    assert cur.fetchone() is None, "Entry was not removed from node_pipeline_relation table."

    # Check that the tag is removed from node_tags
    cur.execute("SELECT * FROM node_tags WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    assert cur.fetchone() is None, "Tag was not removed from node_tags table."

    # Check that rowcount is at least 2 (one for node_pipeline_relation, one for node_tags)
    assert rows_deleted >= 1, "Expected at least one row to be deleted."

    conn.close()

def test_replace_node_in_pipeline(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Setup: create nodes, pipeline, relations, node_pipeline_relation, and tags
    old_node_id = generate_node_id()
    new_node_id = f"{old_node_id}_repl"
    parent_id = generate_node_id()
    child_id = generate_node_id()
    pipeline_id = generate_pip_id()
    user = "test_user"
    tag = "test_tag"

    # Add nodes and pipeline
    for node in [old_node_id, parent_id, child_id]:
        pipeline_db.add_node_to_nodes(cur, node_id=node)
    pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add relations: parent -> old_node -> child
    pipeline_db.add_node_relation(cur, child_id=old_node_id, parent_id=parent_id)
    pipeline_db.add_node_relation(cur, child_id=child_id, parent_id=old_node_id)

    # Add entry and tag for old_node
    pipeline_db.add_node_to_pipeline(cur, node_id=old_node_id, pipeline_id=pipeline_id, user=user)
    pipeline_db.add_node_tag(cur, node_id=old_node_id, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Call the function under test
    result_new_node_id = pipeline_db.replace_node_in_pipeline(cur, old_node_id, new_node_id, pipeline_id)
    conn.commit()

    # Check new node exists
    cur.execute("SELECT * FROM nodes WHERE node_id=?", (new_node_id,))
    assert cur.fetchone() is not None, "New node was not created."

    # Check new node has correct parent and child relations
    cur.execute("SELECT parent_id FROM node_relation WHERE child_id=?", (new_node_id,))
    parent_rows = [row[0] for row in cur.fetchall()]
    assert parent_id in parent_rows, "Parent relation not copied to new node."

    cur.execute("SELECT child_id FROM node_relation WHERE parent_id=?", (new_node_id,))
    child_rows = [row[0] for row in cur.fetchall()]
    assert child_id in child_rows, "Child relation not copied to new node."

    # Check new node has entry and tag in the pipeline
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=? AND pipeline_id=?", (new_node_id, pipeline_id))
    assert cur.fetchone() is not None, "Entry for new node not created."

    cur.execute("SELECT * FROM node_tags WHERE node_id=? AND pipeline_id=?", (new_node_id, pipeline_id))
    tag_row = cur.fetchone()
    assert tag_row is not None, "Tag for new node not created."
    assert tag_row[1] == tag, "Tag value for new node does not match."

    # Check old node is removed from node_pipeline_relation and tags for this pipeline
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=? AND pipeline_id=?", (old_node_id, pipeline_id))
    assert cur.fetchone() is None, "Old node entry was not removed from pipeline."

    cur.execute("SELECT * FROM node_tags WHERE node_id=? AND pipeline_id=?", (old_node_id, pipeline_id))
    assert cur.fetchone() is None, "Old node tag was not removed from pipeline."

    # The function should return the new node id
    assert result_new_node_id == new_node_id, "Returned new_node_id does not match expected value."

    conn.close()

def test_get_pipelines_with_node(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and multiple pipelines
    node_id = generate_node_id()
    pipeline_ids = [generate_pip_id() for _ in range(3)]
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    for pipeline_id in pipeline_ids:
        pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag=f"pipeline_{pipeline_id}")
        pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
    conn.commit()

    # Call the function to get pipelines associated with the node
    pipelines_with_node = pipeline_db.get_pipelines_with_node(cur, node_id)

    # Verify the pipelines match the expected data
    assert set(pipelines_with_node) == set(pipeline_ids), f"Expected pipelines {pipeline_ids}, got {pipelines_with_node}"

    # Test with a node not associated with any pipeline
    new_node_id = generate_node_id()
    pipeline_db.add_node_to_nodes(cur, node_id=new_node_id)
    conn.commit()
    pipelines_with_new_node = pipeline_db.get_pipelines_with_node(cur, new_node_id)
    assert pipelines_with_new_node == [], f"Expected no pipelines for node {new_node_id}, got {pipelines_with_new_node}"

    conn.close()

def test_count_pipeline_with_node(pip_settings):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import pipeline_db

    db_path = pip_settings["connection_db_filepath"]
    conn = pipeline_db.init_db(db_path)
    cur = conn.cursor()

    # Create a node and multiple pipelines
    node_id = generate_node_id()
    pipeline_ids = [generate_pip_id() for _ in range(3)]
    pipeline_db.add_node_to_nodes(cur, node_id=node_id)
    for pipeline_id in pipeline_ids:
        pipeline_db.add_pipeline(cur, pipeline_id=pipeline_id, tag=f"pipeline_{pipeline_id}")
        pipeline_db.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
    conn.commit()

    # Call the function to count pipelines associated with the node
    pipeline_count = pipeline_db.count_pipeline_with_node(cur, node_id)

    # Verify the count matches the expected value
    assert pipeline_count == len(pipeline_ids), f"Expected {len(pipeline_ids)} pipelines, got {pipeline_count}"

    # Test with a node not associated with any pipeline
    new_node_id = generate_node_id()
    pipeline_db.add_node_to_nodes(cur, node_id=new_node_id)
    conn.commit()
    new_node_pipeline_count = pipeline_db.count_pipeline_with_node(cur, new_node_id)
    assert new_node_pipeline_count == 0, f"Expected 0 pipelines for node {new_node_id}, got {new_node_pipeline_count}"

    conn.close()
