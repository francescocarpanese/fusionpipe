import pytest
import tempfile
import os

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

    # Add entry to entries table
    user = "test_user"
    entry_id = pipeline_db.add_node_to_entries(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Remove the entry
    rows_deleted = remove_node_from_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
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
        pipeline_db.add_node_to_entries(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
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
    pipeline_db.add_node_to_entries(cur, node_id=node_ids[0], pipeline_id=pipeline_id, user="test_user")
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

    # Add the node to entries
    pipeline_db.add_node_to_entries(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")

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
    tables_to_check = ["nodes", "entries", "node_tags", "node_relation"]
    for table in tables_to_check:
        cur.execute(f"SELECT * FROM {table} WHERE id=? OR node_id=? OR child_id=? OR parent_id=?", (node_id, node_id, node_id, node_id))
        result = cur.fetchone()
        assert result is None, f"Node {node_id} was not fully removed from table {table}."

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

    # Add the node to entries
    user = "test_user"
    entry_id = pipeline_db.add_node_to_entries(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Remove the node from entries
    rows_deleted = pipeline_db.remove_node_from_entries(cur, node_id=node_id)
    conn.commit()
    assert rows_deleted == 1, "Entry was not removed from the database."

    # Verify the entry is removed
    cur.execute("SELECT * FROM entries WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    result = cur.fetchone()
    conn.close()
    assert result is None, "Entry was not removed from the database."
