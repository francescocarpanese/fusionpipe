import pytest
import tempfile
import os


def test_add_pipeline(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_pip_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    pip_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pip_id, tag="test_pipeline", owner="test_user", notes="This is a test pipeline.")
    
    # Commit and close the connection
    conn.commit()

    # Check if the pipeline was added
    cur = conn.cursor()
    cur.execute("SELECT * FROM pipelines WHERE pipeline_id=%s", (pip_id,))
    result = cur.fetchone()
    
    assert result is not None, f"Pipeline {pip_id} was not added to the database."
    assert result[1] == "test_pipeline", "Pipeline tag does not match expected value."
    assert result[2] == "test_user", "Pipeline owner does not match expected value."
    assert result[3] == "This is a test pipeline.", "Pipeline notes do not match expected value."

def test_add_node(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id

    # Initialize the PostgreSQL test database connection
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    
    # Commit the transaction
    conn.commit()

    # Check if the node was added
    cur = conn.cursor()
    cur.execute("SELECT * FROM nodes WHERE node_id=%s", (node_id,))
    result = cur.fetchone()

    assert result[5] == node_id, "Node tag in entry does not match expected value (should be equal to node_id)."
    
    assert result is not None, f"Node {node_id} was not added to the database."


def test_add_node_to_pipline(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add entry to node_pipeline_relation table
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Check if the entry was added
    cur = conn.cursor()
    cur.execute("SELECT node_id, pipeline_id FROM node_pipeline_relation WHERE node_id=%s and pipeline_id=%s", (node_id, pipeline_id))
    result = cur.fetchone()
    assert result is not None, "Entry was not added to the database."
    assert result[0] == node_id, "Node ID in entry does not match."
    assert result[1] == pipeline_id, "Pipeline ID in entry does not match."

def test_remove_node_from_pipeline(pg_test_db):
    from fusionpipe.utils.db_utils import remove_node_from_pipeline, get_rows_with_node_id_in_entries, get_rows_node_id_in_nodes, is_node_editable
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node and two pipelines
    node_id = generate_node_id()
    pipeline_id1 = generate_pip_id()
    pipeline_id2 = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id1, tag="test_pipeline_1")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id2, tag="test_pipeline_2")
    # Add a tag to the node
    tag = "test_tag"

    # Add entry to node_pipeline_relation table for both pipelines
    entry_id1 = db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id1)
    entry_id2 = db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id2)
    conn.commit()

    # Remove the entry from the first pipeline
    cur = conn.cursor()
    rows_deleted = remove_node_from_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id1)
    conn.commit()

    # The node should still exist in the nodes table
    assert db_utils.get_rows_node_id_in_nodes(cur, node_id), "Node was unexpectedly removed from nodes table."

    # The node should still have an entry in node_pipeline_relation for the second pipeline
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=%s AND pipeline_id=%s", (node_id, pipeline_id2))
    assert cur.fetchone() is not None, "Node entry for second pipeline was removed unexpectedly."

    # The node should be editable only if it is present in one or zero pipelines
    editable = is_node_editable(cur, node_id)
    cur.execute("SELECT COUNT(*) FROM node_pipeline_relation WHERE node_id=%s", (node_id,))
    count = cur.fetchone()[0]
    if count <= 1:
        assert editable is True, "Node should be editable when present in one or zero pipelines."
    else:
        assert editable is False, "Node should not be editable when present in more than one pipeline."

    # Now remove from the second pipeline as well
    rows_deleted2 = remove_node_from_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id2)
    conn.commit()

    # Node should still exist in nodes table
    assert db_utils.get_rows_node_id_in_nodes(cur, node_id), "Node was unexpectedly removed from nodes table after removing from all pipelines."

    # Node should now be editable (since it's not in any pipeline)
    editable = is_node_editable(cur, node_id)
    assert editable is True, "Node should be editable when not present in any pipeline."



def test_add_node_relation(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create two nodes
    child_id = generate_node_id()
    parent_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=child_id)
    db_utils.add_node_to_nodes(cur, node_id=parent_id)

    # Add relation
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()

    # Check that only one relation exists and it matches the inserted values
    cur.execute("SELECT child_id, parent_id FROM node_relation")
    results = cur.fetchall()
    assert len(results) == 1, f"Expected only one relation, found {len(results)}"
    assert results[0][0] == child_id, "Child ID in node relation does not match."
    assert results[0][1] == parent_id, "Parent ID in node relation does not match."

def test_get_node_parents(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create 3 nodes
    node1 = generate_node_id()
    node2 = generate_node_id()
    node3 = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node1)
    db_utils.add_node_to_nodes(cur, node_id=node2)
    db_utils.add_node_to_nodes(cur, node_id=node3)

    # Add relation: node1 is parent of node2
    db_utils.add_node_relation(cur, child_id=node2, parent_id=node1)
    conn.commit()

    # node2 should have node1 as parent
    parents_node2 = db_utils.get_node_parents(cur, node2)
    assert parents_node2 == [node1], f"Expected parent of node2 to be [{node1}], got {parents_node2}"

    # node1 should have no parents
    parents_node1 = db_utils.get_node_parents(cur, node1)
    assert parents_node1 == [], f"Expected no parents for node1, got {parents_node1}"

    # node3 should have no parents
    parents_node3 = db_utils.get_node_parents(cur, node3)
    assert parents_node3 == [], f"Expected no parents for node3, got {parents_node3}"


def test_get_node_children(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create 3 nodes
    node1 = generate_node_id()
    node2 = generate_node_id()
    node3 = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node1)
    db_utils.add_node_to_nodes(cur, node_id=node2)
    db_utils.add_node_to_nodes(cur, node_id=node3)

    # Add relation: node1 is parent of node2
    db_utils.add_node_relation(cur, child_id=node2, parent_id=node1)
    # Add relation: node1 is parent of node3
    db_utils.add_node_relation(cur, child_id=node3, parent_id=node1)
    conn.commit()

    # node1 should have node2 and node3 as children
    children_node1 = db_utils.get_node_children(cur, node1)
    assert set(children_node1) == set([node2, node3]), f"Expected children of node1 to be [{node2}, {node3}], got {children_node1}"

    # node2 should have no children
    children_node2 = db_utils.get_node_children(cur, node2)
    assert children_node2 == [], f"Expected no children for node2, got {children_node2}"

    # node3 should have no children
    children_node3 = db_utils.get_node_children(cur, node3)
    assert children_node3 == [], f"Expected no children for node3, got {children_node3}"


def test_update_node_status(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, NodeState
    from fusionpipe.utils import db_utils


    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    conn.commit()

    # Update node status to RUNNING
    db_utils.update_node_status(cur, node_id=node_id, status=NodeState.RUNNING.value)
    conn.commit()

    # Check if the status was updated
    cur.execute("SELECT status FROM nodes WHERE node_id=%s", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found in database."
    assert result[0] == NodeState.RUNNING.value, f"Expected status {NodeState.RUNNING.value}, got {result[0]}"

    conn.close()



def test_get_pipeline_tag(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_pip_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a pipeline with a specific tag
    pipeline_id = generate_pip_id()
    tag = "my_test_tag"
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Test get_pipeline_tag returns the correct tag
    fetched_tag = db_utils.get_pipeline_tag(cur, pipeline_id)
    assert fetched_tag == tag, f"Expected tag '{tag}', got '{fetched_tag}'"

    # Test get_pipeline_tag returns None for non-existent pipeline
    non_existent_id = "nonexistent_id"
    assert db_utils.get_pipeline_tag(cur, non_existent_id) is None

    conn.close()



def test_get_all_nodes_from_pip_id(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a pipeline and nodes
    pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(3)]
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()


    result_nodes = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)
    assert set(result_nodes) == set(node_ids), f"Expected nodes {node_ids}, got {result_nodes}"

    # Test with a pipeline that has no nodes
    empty_pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=empty_pipeline_id, tag="empty_pipeline")
    conn.commit()
    result_empty = db_utils.get_all_nodes_from_pip_id(cur, empty_pipeline_id)
    assert result_empty == [], "Expected empty list for pipeline with no nodes"

    conn.close()



def test_get_nodes_without_pipeline(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create nodes
    node_ids = [generate_node_id() for _ in range(3)]
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id)

    # Create a pipeline and associate one node with it
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=node_ids[0], pipeline_id=pipeline_id)
    conn.commit()

    # Get nodes without a pipeline
    nodes_without_pipeline = db_utils.get_nodes_without_pipeline(cur)

    # Check that only the nodes not associated with a pipeline are returned
    expected_nodes = set(node_ids[1:])
    assert set(nodes_without_pipeline) == expected_nodes, f"Expected nodes without pipeline: {expected_nodes}, got: {nodes_without_pipeline}"

def test_remove_node_from_relations(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils


    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create nodes
    parent_id = generate_node_id()
    child_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=parent_id)
    db_utils.add_node_to_nodes(cur, node_id=child_id)

    # Add relation
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()

    # Remove the relation
    rows_deleted = db_utils.remove_node_from_relations(cur, node_id=child_id)
    conn.commit()
    assert rows_deleted == 1, "Relation was not removed from the database."

    # Check if the relation was actually removed
    cur.execute("SELECT * FROM node_relation WHERE child_id=%s OR parent_id=%s", (child_id, child_id))
    result = cur.fetchone()
    assert result is None, "Relation was not removed from the database."

def test_remove_node_from_everywhere(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add the node to node_pipeline_relation
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)

    # Add a relation involving the node
    parent_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=parent_id)
    db_utils.add_node_relation(cur, child_id=node_id, parent_id=parent_id)
    conn.commit()

    # Remove the node from everywhere
    db_utils.remove_node_from_everywhere(cur, node_id=node_id)
    conn.commit()
    cur = conn.cursor()

    # Verify the node is removed from all relevant tables
    assert db_utils.get_rows_with_node_id_in_entries(cur, node_id) == [], "Node was not removed from node_pipeline_relation."
    assert db_utils.get_rows_node_id_in_nodes(cur, node_id) == [], "Node was not removed from nodes."
    assert db_utils.get_rows_with_node_id_relations(cur, node_id) == [], "Node was not removed from relations."

def from_node_pipeline_relation(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add the node to node_pipeline_relation
    entry_id = db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Remove the node from node_pipeline_relation
    rows_deleted = db_utils.remove_node_from_node_pipeline_relation(cur, node_id=node_id)
    conn.commit()
    assert rows_deleted == 1, "Entry was not removed from the database."

    # Verify the entry is removed
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=%s AND pipeline_id=%s", (node_id, pipeline_id))
    result = cur.fetchone()
    assert result is None, "Entry was not removed from the database."


def test_get_rows_with_node_id_in_node_pipeline(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add the node to node_pipeline_relation
    entry_id = db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Get rows with the node ID in node_pipeline_relation
    rows = db_utils.get_rows_with_node_id_in_entries(cur, node_id)
    assert len(rows) == 1, "Expected one entry with the node ID."
    assert rows[0][0] == node_id, "Node ID in entry does not match expected value."
    assert rows[0][1] == pipeline_id, "Pipeline ID in entry does not match expected value."

def test_get_rows_node_id_in_nodes(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    conn.commit()

    # Get rows with the node ID in nodes
    rows = db_utils.get_rows_node_id_in_nodes(cur, node_id)
    assert len(rows) == 1, "Expected one row with the node ID."
    assert rows[0][0] == node_id, "Node ID in row does not match expected value."


def test_get_rows_with_node_id_relations(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create two nodes and a relation
    parent_id = generate_node_id()
    child_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=parent_id)
    db_utils.add_node_to_nodes(cur, node_id=child_id)
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()

    # Get rows with the child node ID in relations
    rows = db_utils.get_rows_with_node_id_relations(cur, child_id)

    assert len(rows) == 1, "Expected one row with the child node ID in relations."

def test_get_rows_with_pipeline_id_in_node_pipeline(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a pipeline and nodes
    pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(2)]
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Get rows with the pipeline ID in node_pipeline_relation
    rows = db_utils.get_rows_with_pipeline_id_in_entries(cur, pipeline_id)

    # Verify the rows match the expected data
    assert len(rows) == len(node_ids), f"Expected {len(node_ids)} rows, got {len(rows)}."
    for row in rows:
        assert row[1] == pipeline_id, f"Pipeline ID in row does not match expected value {pipeline_id}."
        assert row[0] in node_ids, f"Node ID {row[3]} in row is not in the expected node IDs {node_ids}."


def test_get_rows_with_pipeline_id_in_pipelines(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils


    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a pipeline
    pipeline_id = generate_pip_id()
    tag = "test_pipeline"
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Get rows with the pipeline ID in pipelines
    rows = db_utils.get_rows_with_pipeline_id_in_pipelines(cur, pipeline_id)

    # Verify the rows match the expected data
    assert len(rows) == 1, f"Expected one row, got {len(rows)}."
    assert rows[0][0] == pipeline_id, f"Pipeline ID in row does not match expected value {pipeline_id}."
    assert rows[0][1] == tag, f"Pipeline tag in row does not match expected value {tag}."



def test_duplicate_pipeline(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.db_utils import add_pipeline_to_pipelines, add_node_to_pipeline, duplicate_pipeline, add_node_to_nodes

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a source pipeline
    source_pipeline_id = "source_pipeline"
    add_pipeline_to_pipelines( cur,
                  pipeline_id=source_pipeline_id,
                  tag="v1.0",
                  owner="group1",
                  notes="This is the source pipeline."
                )

    # Add nodes and node_pipeline_relation to the source pipeline
    add_node_to_nodes(cur, node_id="node1", node_tag="node1_t")
    add_node_to_nodes(cur, node_id="node2", node_tag="node2_t")
    add_node_to_pipeline(cur, node_id="node1", pipeline_id=source_pipeline_id)
    add_node_to_pipeline(cur, node_id="node2", pipeline_id=source_pipeline_id)

    # Commit changes
    conn.commit()

    # Duplicate the pipeline
    new_pipeline_id = "new_pipeline"
    duplicate_pipeline(cur, source_pipeline_id, new_pipeline_id)

    # Verify the new pipeline exists in the pipelines table
    cur.execute("SELECT * FROM pipelines WHERE pipeline_id = %s", (new_pipeline_id,))
    new_pipeline = cur.fetchone()
    assert new_pipeline is not None, "New pipeline was not created."
    assert new_pipeline[0] == new_pipeline_id
    assert new_pipeline[1] == new_pipeline_id  # Tag should match the source pipeline
    assert new_pipeline[2] == "group1"  # Owner should match the source pipeline
    assert new_pipeline[3] == "This is the source pipeline."  # Notes should match the source pipeline

    # Verify the node_pipeline_relation table
    cur.execute("SELECT * FROM node_pipeline_relation WHERE pipeline_id = %s", (new_pipeline_id,))
    node_pipeline_relation = cur.fetchall()
    assert len(node_pipeline_relation) == 2, "Entries were not duplicated correctly."
    assert node_pipeline_relation[0][0] == "node1" # Check first node
    assert node_pipeline_relation[1][0] == "node2" # Check second node


def test_duplicate_pipeline_graph_comparison(pg_test_db, dag_dummy_1):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import pipeline_graph_to_db, db_to_pipeline_graph_from_pip_id
    import networkx as nx

    # Setup database
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the original graph to the database
    original_graph = dag_dummy_1
    pipeline_graph_to_db(original_graph, cur)
    conn.commit()

    # Duplicate the pipeline
    original_pipeline_id = original_graph.graph['pipeline_id']
    new_pipeline_id = "duplicated_pipeline"
    db_utils.duplicate_pipeline(cur, original_pipeline_id, new_pipeline_id)
    conn.commit()

    # Load the original and duplicated pipelines as graphs
    original_graph_loaded = db_to_pipeline_graph_from_pip_id(cur, original_pipeline_id)
    duplicated_graph_loaded = db_to_pipeline_graph_from_pip_id(cur, new_pipeline_id)

    # Compare the graphs
    assert nx.is_isomorphic(
        original_graph_loaded, duplicated_graph_loaded,
        node_match=lambda n1, n2: n1['status'] == n2['status']
    ), "Duplicated graph does not match the original graph structure and attributes."

    # Ensure the pipeline IDs are different
    assert original_graph_loaded.graph['pipeline_id'] != duplicated_graph_loaded.graph['pipeline_id'], \
        "Pipeline IDs should be different between the original and duplicated graphs."


def test_dupicate_node_in_pipeline_full_coverage(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create original node and another node (for relation)
    original_node_id = generate_node_id()
    other_node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=original_node_id, node_tag="test_tag")
    db_utils.add_node_to_nodes(cur, node_id=other_node_id)

    # Add a relation: original_node_id is child, other_node_id is parent
    db_utils.add_node_relation(cur, child_id=original_node_id, parent_id=other_node_id)

    # Create a pipeline and add an entry connecting the pipeline to the node
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=original_node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Duplicate the node
    duplicated_node_id = f"{original_node_id}_copy"
    db_utils.dupicate_node_in_pipeline(cur, original_node_id, duplicated_node_id, pipeline_id, pipeline_id)
    conn.commit()

    # Check nodes table
    cur.execute("SELECT * FROM nodes WHERE node_id = %s", (duplicated_node_id,))
    duplicated_node = cur.fetchone()
    assert duplicated_node is not None, "Duplicated node was not created."

    # Check node_pipeline_relation table
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id = %s AND pipeline_id = %s", (duplicated_node_id, pipeline_id,))
    duplicated_entry = cur.fetchone()
    assert duplicated_entry is not None, "Duplicated node entry was not created."
    assert duplicated_entry[1] == pipeline_id, "Duplicated entry pipeline_id does not match original."

@pytest.mark.parametrize(["parents","childrens"],
            [
             (True, False),  # Copy parents only
             (True, False),
             (False, True),  # Copy children only
            ]
)
def test_copy_node_relations(pg_test_db, parents, childrens):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create nodes
    source_node_id = generate_node_id()
    parent1_id = generate_node_id()
    parent2_id = generate_node_id()
    child1_id = generate_node_id()
    child2_id = generate_node_id()
    new_node_id = generate_node_id()

    # Add nodes to the database
    for node_id in [source_node_id, parent1_id, parent2_id, child1_id, child2_id, new_node_id]:
        db_utils.add_node_to_nodes(cur, node_id=node_id)

    # Add parent relations (source_node_id is child of parent1 and parent2)
    db_utils.add_node_relation(cur, child_id=source_node_id, parent_id=parent1_id)
    db_utils.add_node_relation(cur, child_id=source_node_id, parent_id=parent2_id)

    # Add child relations (child1 and child2 are children of source_node_id)
    db_utils.add_node_relation(cur, child_id=child1_id, parent_id=source_node_id)
    db_utils.add_node_relation(cur, child_id=child2_id, parent_id=source_node_id)
    conn.commit()

    # Copy relations from source_node_id to new_node_id
    db_utils.copy_node_relations(cur, source_node_id, new_node_id, parents=parents, childrens=childrens)
    conn.commit()

    if parents:
        # Check parent relations for new_node_id (should match source_node_id's parents)
        cur.execute("SELECT parent_id FROM node_relation WHERE child_id=%s", (new_node_id,))
        parent_rows = cur.fetchall()
        parent_ids = {row[0] for row in parent_rows}
        assert parent1_id in parent_ids and parent2_id in parent_ids, \
            f"Parent relations not copied correctly: {parent_ids}"
        
    if childrens:
        # Check child relations for new_node_id (should match source_node_id's children)
        cur.execute("SELECT child_id FROM node_relation WHERE parent_id=%s", (new_node_id,))
        child_rows = cur.fetchall()
        child_ids = {row[0] for row in child_rows}
        assert child1_id in child_ids and child2_id in child_ids, \
            f"Child relations not copied correctly: {child_ids}"



def test_duplicate_node_in_pipeline_with_relations(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create nodes
    source_node_id = generate_node_id()
    parent_id = generate_node_id()
    child_id = generate_node_id()
    new_node_id = f"{source_node_id}_copy"

    # Add nodes to the database
    for node_id in [source_node_id, parent_id, child_id]:
        db_utils.add_node_to_nodes(cur, node_id=node_id)

    # Add parent and child relations for the source node
    db_utils.add_node_relation(cur, child_id=source_node_id, parent_id=parent_id)
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=source_node_id)
    conn.commit()

    # Create a pipeline and add an entry connecting the pipeline to the source node
    pipeline_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Duplicate node with relations
    db_utils.duplicate_node_in_pipeline_with_relations(cur, source_node_id, new_node_id, pipeline_id, pipeline_id, parents=True, childrens=True)
    conn.commit()

    # Check that the new node exists
    cur.execute("SELECT * FROM nodes WHERE node_id = %s", (new_node_id,))
    assert cur.fetchone() is not None, "Duplicated node was not created."

    # Check parent relations for new node
    cur.execute("SELECT parent_id FROM node_relation WHERE child_id = %s", (new_node_id,))
    parent_rows = cur.fetchall()
    parent_ids = {row[0] for row in parent_rows}
    assert parent_id in parent_ids, "Parent relation was not copied to duplicated node."

    # Check child relations for new node
    cur.execute("SELECT child_id FROM node_relation WHERE parent_id = %s", (new_node_id,))
    child_rows = cur.fetchall()
    child_ids = {row[0] for row in child_rows}
    assert child_id in child_ids, "Child relation was not copied to duplicated node."



def test_get_pipelines_with_node(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node and multiple pipelines
    node_id = generate_node_id()
    pipeline_ids = [generate_pip_id() for _ in range(3)]
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    for pipeline_id in pipeline_ids:
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=f"pipeline_{pipeline_id}")
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Call the function to get pipelines associated with the node
    pipelines_with_node = db_utils.get_pipelines_with_node(cur, node_id)

    # Verify the pipelines match the expected data
    assert set(pipelines_with_node) == set(pipeline_ids), f"Expected pipelines {pipeline_ids}, got {pipelines_with_node}"

    # Test with a node not associated with any pipeline
    new_node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=new_node_id)
    conn.commit()
    pipelines_with_new_node = db_utils.get_pipelines_with_node(cur, new_node_id)
    assert pipelines_with_new_node == [], f"Expected no pipelines for node {new_node_id}, got {pipelines_with_new_node}"


def test_count_pipeline_with_node(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node and multiple pipelines
    node_id = generate_node_id()
    pipeline_ids = [generate_pip_id() for _ in range(3)]
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    for pipeline_id in pipeline_ids:
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=f"pipeline_{pipeline_id}")
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Call the function to count pipelines associated with the node
    pipeline_count = db_utils.count_pipeline_with_node(cur, node_id)

    # Verify the count matches the expected value
    assert pipeline_count == len(pipeline_ids), f"Expected {len(pipeline_ids)} pipelines, got {pipeline_count}"

    # Test with a node not associated with any pipeline
    new_node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=new_node_id)
    conn.commit()
    new_node_pipeline_count = db_utils.count_pipeline_with_node(cur, new_node_id)
    assert new_node_pipeline_count == 0, f"Expected 0 pipelines for node {new_node_id}, got {new_node_pipeline_count}"

def test_is_node_editable(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)

    # Test case: Node does not belong to any pipeline
    editable = db_utils.is_node_editable(cur, node_id)
    assert editable is True, f"Expected node {node_id} to be editable when not associated with any pipeline."

    # Test case: Node belongs to one pipeline
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    editable = db_utils.is_node_editable(cur, node_id)
    assert editable is True, f"Expected node {node_id} to be editable when associated with one pipeline."

    # Test case: Node belongs to more than one pipeline
    another_pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=another_pipeline_id, tag="another_test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=another_pipeline_id)
    conn.commit()

    editable = db_utils.is_node_editable(cur, node_id)
    assert editable is False, f"Expected node {node_id} to be non-editable when associated with more than one pipeline."


def test_update_editable_status_for_all_nodes(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create nodes
    node1 = generate_node_id()
    node2 = generate_node_id()
    node3 = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node1)
    db_utils.add_node_to_nodes(cur, node_id=node2)
    db_utils.add_node_to_nodes(cur, node_id=node3)

    # Create pipelines
    pipeline1 = generate_pip_id()
    pipeline2 = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline1, tag="pipeline1")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline2, tag="pipeline2")

    # Associate nodes with pipelines
    db_utils.add_node_to_pipeline(cur, node_id=node1, pipeline_id=pipeline1)
    db_utils.add_node_to_pipeline(cur, node_id=node2, pipeline_id=pipeline1)
    db_utils.add_node_to_pipeline(cur, node_id=node2, pipeline_id=pipeline2)
    conn.commit()

    # Call the function to update editable status
    db_utils.update_editable_status_for_all_nodes(cur)
    conn.commit()

    # Verify editable status for each node
    editable_node1 = db_utils.is_node_editable(cur, node1)
    assert editable_node1 is True, f"Expected node1 to be editable, got {editable_node1}"

    editable_node2 =  db_utils.is_node_editable(cur, node2)
    assert editable_node2 is False, f"Expected node2 to be non-editable, got {editable_node2}"

    editable_node3 = db_utils.is_node_editable(cur, node3)
    assert editable_node3 is True, f"Expected node3 to be editable, got {editable_node3}"


def test_get_pipeline_notes_existing_and_nonexistent(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a pipeline with notes
    pipeline_id = generate_pip_id()
    notes = "This is a test note."
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", notes=notes)
    conn.commit()

    # Test: get notes for existing pipeline
    fetched_notes = db_utils.get_pipeline_notes(cur, pipeline_id)
    assert fetched_notes == notes, f"Expected notes '{notes}', got '{fetched_notes}'"

    # Test: get notes for pipeline with no notes
    pipeline_id_no_notes = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id_no_notes, tag="no_notes_pipeline")
    conn.commit()
    fetched_notes_none = db_utils.get_pipeline_notes(cur, pipeline_id_no_notes)
    assert fetched_notes_none is None, "Expected None for pipeline with no notes"

    # Test: get notes for non-existent pipeline
    non_existent_id = "nonexistent_id"
    assert db_utils.get_pipeline_notes(cur, non_existent_id) is None

def test_get_pipeline_owner_existing_and_nonexistent(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a pipeline with an owner
    pipeline_id = generate_pip_id()
    owner = "test_owner"
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", owner=owner)
    conn.commit()

    # Test: get owner for existing pipeline
    fetched_owner = db_utils.get_pipeline_owner(cur, pipeline_id)
    assert fetched_owner == owner, f"Expected owner '{owner}', got '{fetched_owner}'"

    # Test: get owner for pipeline with no owner
    pipeline_id_no_owner = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id_no_owner, tag="no_owner_pipeline")
    conn.commit()
    fetched_owner_none = db_utils.get_pipeline_owner(cur, pipeline_id_no_owner)
    assert fetched_owner_none is None, "Expected None for pipeline with no owner"

    # Test: get owner for non-existent pipeline
    non_existent_id = "nonexistent_id"
    assert db_utils.get_pipeline_owner(cur, non_existent_id) is None

def test_get_node_notes_existing_and_nonexistent(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a node with notes
    node_id = generate_node_id()
    notes = "This is a test node note."
    cur.execute("INSERT INTO nodes (node_id, notes) VALUES (%s, %s)", (node_id, notes))
    conn.commit()

    # Test: get notes for existing node
    fetched_notes = db_utils.get_node_notes(cur, node_id)
    assert fetched_notes == notes, f"Expected notes '{notes}', got '{fetched_notes}'"

    # Test: get notes for node with no notes
    node_id_no_notes = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id_no_notes)
    conn.commit()
    fetched_notes_none = db_utils.get_node_notes(cur, node_id_no_notes)
    assert fetched_notes_none is None, "Expected None for node with no notes"

    # Test: get notes for non-existent node
    non_existent_id = "nonexistent_node"
    assert db_utils.get_node_notes(cur, non_existent_id) is None


def test_remove_pipeline_removes_pipeline_from_everywhere(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils
    
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a pipeline and related data
    pipeline_id = generate_pip_id()
    node_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", owner="owner", notes="notes")
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Remove the pipeline
    db_utils.remove_pipeline_from_everywhere(cur, pipeline_id)
    conn.commit()

    # Check that the pipeline is removed
    cur.execute("SELECT * FROM pipelines WHERE pipeline_id=%s", (pipeline_id,))
    assert cur.fetchone() is None, "Pipeline was not removed from pipelines table."

    # # Check that related data is NOT automatically removed (no ON DELETE CASCADE)
    # cur.execute("SELECT * FROM node_pipeline_relation WHERE pipeline_id=%s", (pipeline_id,))
    # assert cur.fetchone() is not None, "node_pipeline_relation entry was unexpectedly removed."

    # Node should still exist
    cur.execute("SELECT * FROM nodes WHERE node_id=%s", (node_id,))
    assert cur.fetchone() is not None, "Node was unexpectedly removed."

def test_sanitize_node_relation(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create nodes and pipeline
    node1 = 'n1'
    node2 = 'n2'
    node3 = 'n3'
    node4 = 'n4'
    node5 = 'n5'  # This node will not be editable
    node6 = 'n6'  # This node will not be editable
    node7 = 'n7'  # This node will be a child of node6
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node1)
    db_utils.add_node_to_nodes(cur, node_id=node2)
    db_utils.add_node_to_nodes(cur, node_id=node3)
    db_utils.add_node_to_nodes(cur, node_id=node4)
    db_utils.add_node_to_nodes(cur, node_id=node5, editable=False)  # Not editable
    db_utils.add_node_to_nodes(cur, node_id=node6, editable=False)  # Not editable
    db_utils.add_node_to_nodes(cur, node_id=node7)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=node1, pipeline_id=pipeline_id)
    db_utils.add_node_to_pipeline(cur, node_id=node2, pipeline_id=pipeline_id)
    db_utils.add_node_to_pipeline(cur, node_id=node7, pipeline_id=pipeline_id)
    # node 1,2,3,7 are in the pipeline
    # node3, node4, node5, node6 are not in the pipeline
    # node5 and node6 are not editable
    # node6 is parent of node 7

    # Add relations:
    # node1 -> node2 (both in pipeline, should remain)
    db_utils.add_node_relation(cur, child_id=node2, parent_id=node1)
    # node3 -> node4 (neither in pipeline, should remain untouched)
    db_utils.add_node_relation(cur, child_id=node4, parent_id=node3)
    # node5 -> node6 (not in pipeline, not editable, should be remain)
    db_utils.add_node_relation(cur, child_id=node6, parent_id=node5)
    # node5 -> node7 (parents are not editable and not in pipeline)
    db_utils.add_node_relation(cur, child_id=node7, parent_id=node5)

    conn.commit()

    # Sanitize relations
    db_utils.sanitize_node_relation(cur, pipeline_id)
    conn.commit()

    # Only relation node1->node2 should remain for this pipeline
    cur.execute("SELECT child_id, parent_id FROM node_relation")
    relations = set(tuple(row) for row in cur.fetchall())
    assert (node2, node1) in relations, "Expected relation (node2, node1) to remain"
    assert (node4, node3) in relations, "Relation (node4, node3) should remain (unrelated to pipeline)"
    assert (node6, node5) in relations, "Relation (node5, node6) should remain (not in pipeline)"
    assert (node7, node5) not in relations, "Relation (node7, node6) should be removed (parent nodes are not in the pipeline)"

def test_remove_pipeline_from_everywhere(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a pipeline and related data
    pipeline_id = generate_pip_id()
    node_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", owner="owner", notes="notes")
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Remove the pipeline from everywhere
    db_utils.remove_pipeline_from_everywhere(cur, pipeline_id)
    conn.commit()

    # Check that the pipeline is removed
    cur.execute("SELECT * FROM pipelines WHERE pipeline_id=%s", (pipeline_id,))
    assert cur.fetchone() is None, "Pipeline was not removed from pipelines table."

    # Check that related data is also removed
    cur.execute("SELECT * FROM node_pipeline_relation WHERE pipeline_id=%s", (pipeline_id,))
    assert cur.fetchone() is None, "node_pipeline_relation entry was not removed."

    # Node should still exist
    cur.execute("SELECT * FROM nodes WHERE node_id=%s", (node_id,))
    assert cur.fetchone() is not None, "Node was unexpectedly removed."

def test_can_node_run_logic(pg_test_db):
    """
    Test logic for determining if a node can run based on its status.
    """
    from fusionpipe.utils import pip_utils
    from fusionpipe.utils import pip_utils
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node with default status ('ready') and editable True
    node_id = pip_utils.generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    conn.commit()

    canrun = pip_utils.can_node_run(cur, node_id)
    assert canrun is True, "Node should be able to run when status is 'ready' and editable is True."
    # Update node status to 'running' and check again
    cur.execute("UPDATE nodes SET status='running' WHERE node_id=%s", (node_id,))
    conn.commit()
    canrun = pip_utils.can_node_run(cur, node_id)
    assert canrun is False, "Node should not be able to run when status is 'running'."
    # Update node status to 'failed' and check again
    cur.execute("UPDATE nodes SET status='failed' WHERE node_id=%s", (node_id,))
    conn.commit()
    canrun = pip_utils.can_node_run(cur, node_id)
    assert canrun is False, "Node should not be able to run when status is 'failed'."

def test_update_editable_status(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    conn.commit()

    # Update editable status to False
    rows_updated = db_utils.update_editable_status(cur, node_id=node_id, editable=False)
    conn.commit()

    # Verify the editable status was updated
    assert rows_updated == 1, "Editable status was not updated in the database."
    cur.execute("SELECT editable FROM nodes WHERE node_id=%s", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found in database."
    assert result[0] == 0, f"Expected editable status to be False, got {result[0]}"

    # Update editable status back to True
    rows_updated = db_utils.update_editable_status(cur, node_id=node_id, editable=True)
    conn.commit()

    # Verify the editable status was updated
    assert rows_updated == 1, "Editable status was not updated in the database."
    cur.execute("SELECT editable FROM nodes WHERE node_id=%s", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found in database."
    assert result[0] == 1, f"Expected editable status to be True, got {result[0]}"


def test_duplicate_node_pipeline_relation(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create source pipeline and nodes
    source_pipeline_id = generate_pip_id()
    target_pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(2)]
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=source_pipeline_id, tag="source_pipeline")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=target_pipeline_id, tag="target_pipeline")
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=source_pipeline_id)

    conn.commit()

    # Duplicate node-pipeline relations for both nodes to the target pipeline
    db_utils.duplicate_node_pipeline_relation(cur, source_pipeline_id, node_ids, target_pipeline_id)
    conn.commit()

    # Check that the relations exist in the target pipeline
    for node_id in node_ids:
        cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=%s AND pipeline_id=%s", (node_id, target_pipeline_id))
        result = cur.fetchone()
        assert result is not None, f"Node-pipeline relation for node {node_id} was not duplicated to target pipeline."

def test_add_and_remove_process(pg_test_db):
    from fusionpipe.utils import db_utils
    import datetime

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a process
    process_id = "proc_123"
    node_id = "node_abc"
    status = "running"
    start_time = datetime.datetime.now()
    end_time = None

    db_utils.add_process(cur, process_id, node_id, status, start_time, end_time)
    conn.commit()

    # Check that the process was added
    cur.execute("SELECT * FROM processes WHERE process_id=%s", (process_id,))
    result = cur.fetchone()
    assert result is not None, "Process was not added to the database."
    assert result[0] == process_id, "Process ID does not match."
    assert result[1] == node_id, "Node ID does not match."
    assert result[2] == status, "Status does not match."
    assert result[3] == start_time, "Start time does not match."
    assert result[4] == end_time, "End time does not match."

    # Remove the process
    rows_deleted = db_utils.remove_process(cur, process_id)
    conn.commit()
    assert rows_deleted == 1, "Process was not removed from the database."

    # Check that the process is gone
    cur.execute("SELECT * FROM processes WHERE process_id=%s", (process_id,))
    result = cur.fetchone()
    assert result is None, "Process was not removed from the database."


def test_clear_all_tables(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add data to all tables
    pipeline_id = generate_pip_id()
    node_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    db_utils.add_node_relation(cur, child_id=node_id, parent_id=node_id)
    conn.commit()

    # Call clear_all_tables
    db_utils.clear_all_tables(conn)

    # Check all tables are empty
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pipelines")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT COUNT(*) FROM nodes")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT COUNT(*) FROM projects")
    assert cur.fetchone()[0] == 0   
    cur.execute("SELECT COUNT(*) FROM processes")
    assert cur.fetchone()[0] == 0         
    cur.execute("SELECT COUNT(*) FROM node_pipeline_relation")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT COUNT(*) FROM node_relation")
    assert cur.fetchone()[0] == 0

def test_get_all_tables_names(pg_test_db):
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    db_utils.init_db(conn)

    # Get all table names
    table_names = db_utils.get_all_tables_names(conn)

    # Check that all expected tables are present
    expected_tables = {
        "pipelines",
        "nodes",
        "node_pipeline_relation",
        "node_relation",
        "projects",
        "processes"
    }
    assert expected_tables.issubset(set(table_names)), f"Missing tables: {expected_tables - set(table_names)}"

def test_add_and_remove_project_to_pipeline(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_pip_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project and a pipeline
    project_id = "proj_123"
    pipeline_id = generate_pip_id()
    db_utils.add_project(cur, project_id=project_id, tag="test_project", notes="project notes", owner="owner1")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    conn.commit()

    # Associate project with pipeline
    rows_updated = db_utils.add_project_to_pipeline(cur, project_id, pipeline_id)
    conn.commit()
    assert rows_updated == 1, "Project was not added to pipeline."

    # Verify association
    cur.execute("SELECT project_id FROM pipelines WHERE pipeline_id=%s", (pipeline_id,))
    result = cur.fetchone()
    assert result is not None, "Pipeline not found."
    assert result[0] == project_id, "Project ID not associated with pipeline."

    # Remove project from pipeline
    rows_updated = db_utils.remove_project_from_pipeline(cur, project_id, pipeline_id)
    conn.commit()
    assert rows_updated == 1, "Project was not removed from pipeline."

    # Verify removal
    cur.execute("SELECT project_id FROM pipelines WHERE pipeline_id=%s", (pipeline_id,))
    result = cur.fetchone()
    assert result is not None, "Pipeline not found after removal."
    assert result[0] is None, "Project ID was not removed from pipeline."

def test_get_all_projects(pg_test_db):
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add projects
    projects = [
        {"project_id": "proj1", "tag": "tag1", "notes": "notes1", "owner": "owner1"},
        {"project_id": "proj2", "tag": "tag2", "notes": "notes2", "owner": "owner2"},
    ]
    for p in projects:
        db_utils.add_project(cur, **p)
    conn.commit()

    # Get all projects
    result = db_utils.get_all_projects(cur)
    project_ids = {proj["project_id"] for proj in result}
    assert "proj1" in project_ids and "proj2" in project_ids, f"Projects not found: {project_ids}"

def test_check_project_exists(pg_test_db):
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project
    db_utils.add_project(cur, project_id="proj_exists", tag="tag", notes="notes", owner="owner")
    conn.commit()

    # Check existence
    assert db_utils.check_project_exists(cur, "proj_exists") is True
    assert db_utils.check_project_exists(cur, "proj_missing") is False

def test_get_project_by_id(pg_test_db):
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project
    db_utils.add_project(cur, project_id="proj_id", tag="tag", notes="notes", owner="owner")
    conn.commit()

    # Get project by id
    proj = db_utils.get_project_by_id(cur, "proj_id")
    assert proj is not None, "Project not found"
    assert proj["project_id"] == "proj_id"
    assert proj["tag"] == "tag"
    assert proj["notes"] == "notes"
    assert proj["owner"] == "owner"

    # Non-existent project
    assert db_utils.get_project_by_id(cur, "missing_id") is None

def test_remove_project(pg_test_db):
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add and remove project
    db_utils.add_project(cur, project_id="proj_del", tag="tag", notes="notes", owner="owner")
    conn.commit()
    rows_deleted = db_utils.remove_project(cur, "proj_del")
    conn.commit()
    assert rows_deleted == 1, "Project was not deleted"
    assert db_utils.get_project_by_id(cur, "proj_del") is None


def test_update_project_tag(pg_test_db):
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project
    project_id = "proj_tag_update"
    db_utils.add_project(cur, project_id=project_id, tag="old_tag", notes="notes", owner="owner")
    conn.commit()

    # Update the tag
    rows_updated = db_utils.update_project_tag(cur, project_id, "new_tag")
    conn.commit()
    assert rows_updated == 1, "Project tag was not updated"

    # Verify the tag was updated
    cur.execute("SELECT tag FROM projects WHERE project_id=%s", (project_id,))
    result = cur.fetchone()
    assert result is not None, "Project not found"
    assert result[0] == "new_tag", f"Expected tag 'new_tag', got '{result[0]}'"

def test_update_project_notes(pg_test_db):
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project
    project_id = "proj_notes_update"
    db_utils.add_project(cur, project_id=project_id, tag="tag", notes="old_notes", owner="owner")
    conn.commit()

    # Update the notes
    rows_updated = db_utils.update_project_notes(cur, project_id, "new_notes")
    conn.commit()
    assert rows_updated == 1, "Project notes were not updated"

    # Verify the notes were updated
    cur.execute("SELECT notes FROM projects WHERE project_id=%s", (project_id,))
    result = cur.fetchone()
    assert result is not None, "Project not found"
    assert result[0] == "new_notes", f"Expected notes 'new_notes', got '{result[0]}'"

def test_get_pipeline_ids_by_project(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project
    project_id = "proj_test"
    db_utils.add_project(cur, project_id=project_id, tag="tag", notes="notes", owner="owner")
    conn.commit()

    # Add pipelines and associate them with the project
    pipeline_ids = [generate_pip_id() for _ in range(3)]
    for pid in pipeline_ids:
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pid, tag=f"pipeline_{pid}")
        db_utils.add_project_to_pipeline(cur, project_id, pid)
    conn.commit()

    # Add a pipeline not associated with the project
    unrelated_pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=unrelated_pipeline_id, tag="unrelated")
    conn.commit()

    # Get pipeline IDs by project
    result = db_utils.get_pipeline_ids_by_project(cur, project_id)
    assert set(result) == set(pipeline_ids), f"Expected {pipeline_ids}, got {result}"

    # Test with project that has no pipelines
    db_utils.add_project(cur, project_id="proj_empty", tag="tag", notes="notes", owner="owner")
    conn.commit()
    result_empty = db_utils.get_pipeline_ids_by_project(cur, "proj_empty")
    assert result_empty == [], f"Expected empty list, got {result_empty}"

    # Test with non-existent project
    result_none = db_utils.get_pipeline_ids_by_project(cur, "missing_proj")
    assert result_none == [], f"Expected empty list for missing project, got {result_none}"


def test_remove_project_from_everywhere(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project and pipelines associated with it
    project_id = "proj_remove_everywhere"
    pipeline_ids = [generate_pip_id() for _ in range(2)]
    db_utils.add_project(cur, project_id=project_id, tag="tag", notes="notes", owner="owner")
    for pid in pipeline_ids:
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pid, tag=f"pipeline_{pid}")
        db_utils.add_project_to_pipeline(cur, project_id, pid)
    conn.commit()

    # Remove project from everywhere
    rows_affected = db_utils.remove_project_from_everywhere(cur, project_id)
    conn.commit()

    # Project should be removed from projects table
    assert db_utils.get_project_by_id(cur, project_id) is None, "Project was not removed from projects table."

    # Pipelines should have project_id set to NULL
    for pid in pipeline_ids:
        cur.execute("SELECT project_id FROM pipelines WHERE pipeline_id=%s", (pid,))
        result = cur.fetchone()
        assert result is not None, "Pipeline not found after project removal."
        assert result[0] is None, f"Project ID was not removed from pipeline {pid}."

    # Return value should be 1 (number of rows deleted from projects)
    assert rows_affected == 1, f"Expected 1 row deleted from projects, got {rows_affected}"

def test_get_project_id_by_pipeline(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project and a pipeline, associate them
    project_id = "proj_for_pipeline"
    pipeline_id = generate_pip_id()
    db_utils.add_project(cur, project_id=project_id, tag="tag", notes="notes", owner="owner")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="pipeline_tag")
    db_utils.add_project_to_pipeline(cur, project_id, pipeline_id)
    conn.commit()

    # Should return the correct project_id
    result = db_utils.get_project_id_by_pipeline(cur, pipeline_id)
    assert result == project_id, f"Expected project_id '{project_id}', got '{result}'"

    # Pipeline with no project
    pipeline_id_no_project = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id_no_project, tag="no_project")
    conn.commit()
    result_none = db_utils.get_project_id_by_pipeline(cur, pipeline_id_no_project)
    assert result_none == "", f"Expected empty string for pipeline with no project, got '{result_none}'"

    # Non-existent pipeline
    result_missing = db_utils.get_project_id_by_pipeline(cur, "missing_pipeline")
    assert result_missing == "", f"Expected empty string for missing pipeline, got '{result_missing}'"


def test_add_pipeline_relation_and_remove(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add two pipelines
    parent_id = generate_pip_id()
    child_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_id, tag="parent_pipeline")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=child_id, tag="child_pipeline")
    conn.commit()

    # Add relation between pipelines
    rows_added = db_utils.add_pipeline_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()
    assert rows_added == 1, "Pipeline relation was not added."

    # Verify relation exists
    cur.execute("SELECT child_id, parent_id FROM pipeline_relation WHERE child_id=%s AND parent_id=%s", (child_id, parent_id))
    result = cur.fetchone()
    assert result is not None, "Pipeline relation not found."
    assert result[0] == child_id and result[1] == parent_id, "Pipeline relation values do not match."

    # Remove relation by child_id and parent_id
    rows_removed = db_utils.remove_pipeline_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()
    assert rows_removed == 1, "Pipeline relation was not removed."

    # Verify relation is removed
    cur.execute("SELECT * FROM pipeline_relation WHERE child_id=%s AND parent_id=%s", (child_id, parent_id))
    assert cur.fetchone() is None, "Pipeline relation was not deleted."

def test_add_pipeline_relation_with_null_parent_and_remove(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a pipeline
    child_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=child_id, tag="child_pipeline")
    conn.commit()

    # Add relation with parent_id=None
    rows_added = db_utils.add_pipeline_relation(cur, child_id=child_id, parent_id=None)
    conn.commit()
    assert rows_added == 1, "Pipeline relation with null parent was not added."

    # Verify relation exists
    cur.execute("SELECT child_id, parent_id FROM pipeline_relation WHERE child_id=%s", (child_id,))
    result = cur.fetchone()
    assert result is not None, "Pipeline relation with null parent not found."
    assert result[0] == child_id and result[1] is None, "Pipeline relation values do not match for null parent."

    # Remove relation by child_id only
    rows_removed = db_utils.remove_pipeline_relation(cur, child_id=child_id)
    conn.commit()
    assert rows_removed == 1, "Pipeline relation with null parent was not removed."

    # Verify relation is removed
    cur.execute("SELECT * FROM pipeline_relation WHERE child_id=%s", (child_id,))
    assert cur.fetchone() is None, "Pipeline relation with null parent was not deleted."

def test_get_pipeline_parents(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add pipelines
    parent_id1 = generate_pip_id()
    parent_id2 = generate_pip_id()
    child_id = generate_pip_id()
    unrelated_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_id1, tag="parent1")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_id2, tag="parent2")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=child_id, tag="child")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=unrelated_id, tag="unrelated")
    conn.commit()

    # Add relations: child_id has two parents
    db_utils.add_pipeline_relation(cur, child_id=child_id, parent_id=parent_id1)
    db_utils.add_pipeline_relation(cur, child_id=child_id, parent_id=parent_id2)
    conn.commit()

    # Test: get parents for child_id
    parents = db_utils.get_pipeline_parents(cur, child_id)
    assert set(parents) == set([parent_id1, parent_id2]), f"Expected parents {parent_id1}, {parent_id2}, got {parents}"

    # Test: pipeline with no parents
    no_parents = db_utils.get_pipeline_parents(cur, parent_id1)
    assert no_parents == [], f"Expected no parents for {parent_id1}, got {no_parents}"

    # Test: pipeline with no relation at all
    no_parents_unrelated = db_utils.get_pipeline_parents(cur, unrelated_id)
    assert no_parents_unrelated == [], f"Expected no parents for {unrelated_id}, got {no_parents_unrelated}"

    # Test: pipeline with parent_id=None (root pipeline)
    root_pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=root_pipeline_id, tag="root")
    db_utils.add_pipeline_relation(cur, child_id=root_pipeline_id, parent_id=None)
    conn.commit()
    parents_root = db_utils.get_pipeline_parents(cur, root_pipeline_id)
    assert parents_root == [None], f"Expected [None] for root pipeline, got {parents_root}"


def test_get_all_pipelines_from_project_id(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_pip_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project
    project_id = "proj_for_pipelines"
    db_utils.add_project(cur, project_id=project_id, tag="tag", notes="notes", owner="owner")
    conn.commit()

    # Add pipelines and associate them with the project
    pipeline_ids = ["pipeline1", "pipeline2", "pipeline3"]
    for pid in pipeline_ids:
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pid, tag=f"tag_{pid}")
        db_utils.add_project_to_pipeline(cur, project_id, pid)
    conn.commit()

    # Add a pipeline not associated with the project
    unrelated_pipeline_id = "unrelated_pipeline"
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=unrelated_pipeline_id, tag="unrelated")
    conn.commit()

    # Get all pipelines for the project
    result = db_utils.get_all_pipelines_from_project_id(cur, project_id)
    assert set(result) == set(pipeline_ids), f"Expected {pipeline_ids}, got {result}"

    # Test with project that has no pipelines
    db_utils.add_project(cur, project_id="proj_empty", tag="tag", notes="notes", owner="owner")
    conn.commit()
    result_empty = db_utils.get_all_pipelines_from_project_id(cur, "proj_empty")
    assert result_empty == [], f"Expected empty list, got {result_empty}"

    # Test with non-existent project
    result_none = db_utils.get_all_pipelines_from_project_id(cur, "missing_proj")
    assert result_none == [], f"Expected empty list for missing project, got {result_none}"


def test_is_pipeline_editable(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a pipeline
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    conn.commit()

    # Test case: Pipeline with no nodes
    is_editable = db_utils.is_pipeline_editable(cur, pipeline_id)
    assert is_editable is False, f"Expected pipeline {pipeline_id} to be non-editable when it has no nodes."

    # Add a node to the pipeline
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, editable=True)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Test case: Pipeline with one editable node
    is_editable = db_utils.is_pipeline_editable(cur, pipeline_id)
    assert is_editable is True, f"Expected pipeline {pipeline_id} to be editable when it has at least one editable node."

    # Add another node to the pipeline and set it to non-editable
    another_node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=another_node_id, editable=False)
    db_utils.add_node_to_pipeline(cur, node_id=another_node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Test case: Pipeline with one editable and one non-editable node
    is_editable = db_utils.is_pipeline_editable(cur, pipeline_id)
    assert is_editable is True, f"Expected pipeline {pipeline_id} to remain editable when it has at least one editable node."

    # Update the first node to be non-editable
    db_utils.update_editable_status(cur, node_id=node_id, editable=False)
    conn.commit()

    # Test case: Pipeline with no editable nodes
    is_editable = db_utils.is_pipeline_editable(cur, pipeline_id)
    assert is_editable is False, f"Expected pipeline {pipeline_id} to be non-editable when all its nodes are non-editable."
