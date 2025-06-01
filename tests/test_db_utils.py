import pytest
import tempfile
import os
import sqlite3


def test_add_pipeline(in_memory_db_conn):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_pip_id

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    pip_id = generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pip_id, tag="test_pipeline", owner="test_user", notes="This is a test pipeline.")
    
    # Commit and close the connection
    conn.commit()

    # Check if the pipeline was added
    cur = conn.cursor()
    cur.execute("SELECT * FROM pipelines WHERE pipeline_id=?", (pip_id,))
    result = cur.fetchone()
    
    assert result is not None, f"Pipeline {pip_id} was not added to the database."
    assert result[1] == "test_pipeline", "Pipeline tag does not match expected value."
    assert result[2] == "test_user", "Pipeline owner does not match expected value."
    assert result[3] == "This is a test pipeline.", "Pipeline notes do not match expected value."

def test_add_node(in_memory_db_conn):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    # Initialize the in-memory database connection
    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    
    # Commit and close the connection
    conn.commit()

    # Check if the node was added
    cur = conn.cursor()
    cur.execute("SELECT * FROM nodes WHERE node_id=?", (node_id,))
    result = cur.fetchone()
    
    assert result is not None, f"Node {node_id} was not added to the database."

def test_add_node_to_entries(in_memory_db_conn):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add entry to node_pipeline_relation table
    user = "test_user"
    entry_id = db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Check if the entry was added
    cur = conn.cursor()
    cur.execute("SELECT node_id, pipeline_id, user FROM node_pipeline_relation WHERE id=?", (entry_id,))
    result = cur.fetchone()
    assert result is not None, "Entry was not added to the database."
    assert result[0] == node_id, "Node ID in entry does not match."
    assert result[1] == pipeline_id, "Pipeline ID in entry does not match."
    assert result[2] == user, "User in entry does not match."


def test_remove_node_from_pipeline(in_memory_db_conn):
    from fusionpipe.utils.db_utils import remove_node_from_pipeline
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    # Add a tag to the node
    tag = "test_tag"
    db_utils.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag=tag)

    # Add entry to node_pipeline_relation table
    user = "test_user"
    entry_id = db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
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


def test_add_node_relation(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils
    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create two nodes
    child_id = generate_node_id()
    parent_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=child_id)
    db_utils.add_node_to_nodes(cur, node_id=parent_id)

    # Add relation
    relation_id = db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()

    # Check that the relation exists
    cur.execute("SELECT child_id, parent_id FROM node_relation WHERE id=?", (relation_id,))
    result = cur.fetchone()
    assert result is not None, "Node relation was not added to the database."
    assert result[0] == child_id, "Child ID in node relation does not match."
    assert result[1] == parent_id, "Parent ID in node relation does not match."

def test_get_node_parents(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
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


def test_get_node_children(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
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


def test_update_node_status(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, NodeState
    from fusionpipe.utils import db_utils


    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    conn.commit()

    # Update node status to RUNNING
    db_utils.update_node_status(cur, node_id=node_id, status=NodeState.RUNNING.value)
    conn.commit()

    # Check if the status was updated
    cur.execute("SELECT status FROM nodes WHERE node_id=?", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found in database."
    assert result[0] == NodeState.RUNNING.value, f"Expected status {NodeState.RUNNING.value}, got {result[0]}"

    conn.close()



def test_get_pipeline_tag(in_memory_db_conn):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_pip_id

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Add a pipeline with a specific tag
    pipeline_id = generate_pip_id()
    tag = "my_test_tag"
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Test get_pipeline_tag returns the correct tag
    fetched_tag = db_utils.get_pipeline_tag(cur, pipeline_id)
    assert fetched_tag == tag, f"Expected tag '{tag}', got '{fetched_tag}'"

    # Test get_pipeline_tag returns None for non-existent pipeline
    non_existent_id = "nonexistent_id"
    assert db_utils.get_pipeline_tag(cur, non_existent_id) is None

    conn.close()



def test_get_all_nodes_from_pip_id(in_memory_db_conn):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a pipeline and nodes
    pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(3)]
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
    conn.commit()


    result_nodes = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)
    assert set(result_nodes) == set(node_ids), f"Expected nodes {node_ids}, got {result_nodes}"

    # Test with a pipeline that has no nodes
    empty_pipeline_id = generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=empty_pipeline_id, tag="empty_pipeline")
    conn.commit()
    result_empty = db_utils.get_all_nodes_from_pip_id(cur, empty_pipeline_id)
    assert result_empty == [], "Expected empty list for pipeline with no nodes"

    conn.close()



def test_get_nodes_without_pipeline(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create nodes
    node_ids = [generate_node_id() for _ in range(3)]
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id)

    # Create a pipeline and associate one node with it
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=node_ids[0], pipeline_id=pipeline_id, user="test_user")
    conn.commit()

    # Get nodes without a pipeline
    nodes_without_pipeline = db_utils.get_nodes_without_pipeline(cur)

    # Check that only the nodes not associated with a pipeline are returned
    expected_nodes = set(node_ids[1:])
    assert set(nodes_without_pipeline) == expected_nodes, f"Expected nodes without pipeline: {expected_nodes}, got: {nodes_without_pipeline}"

def test_remove_node_from_tags(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add a tag to the node
    tag = "test_tag"
    db_utils.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Remove the tag
    rows_deleted = db_utils.remove_node_from_tags(cur, node_id=node_id)
    conn.commit()
    assert rows_deleted == 1, "Tag was not removed from the database."

    # Check if the tag was actually removed
    cur.execute("SELECT * FROM node_tags WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    result = cur.fetchone()
    assert result is None, "Tag was not removed from the database."


def test_remove_node_from_relations(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils


    conn = in_memory_db_conn
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
    cur.execute("SELECT * FROM node_relation WHERE child_id=? OR parent_id=?", (child_id, child_id))
    result = cur.fetchone()
    assert result is None, "Relation was not removed from the database."

def test_remove_node_from_everywhere(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add the node to node_pipeline_relation
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")

    # Add a tag to the node
    tag = "test_tag"
    db_utils.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag=tag)

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
    assert db_utils.get_rows_with_node_id_in_node_tags(cur, node_id) == [], "Node was not removed from tags."
    assert db_utils.get_rows_with_node_id_relations(cur, node_id) == [], "Node was not removed from relations."

def test_remove_node_from_entries(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add the node to node_pipeline_relation
    user = "test_user"
    entry_id = db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Remove the node from node_pipeline_relation
    rows_deleted = db_utils.remove_node_from_node_pipeline_relation(cur, node_id=node_id)
    conn.commit()
    assert rows_deleted == 1, "Entry was not removed from the database."

    # Verify the entry is removed
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    result = cur.fetchone()
    assert result is None, "Entry was not removed from the database."


def test_get_rows_with_node_id_in_entries(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add the node to node_pipeline_relation
    user = "test_user"
    entry_id = db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Get rows with the node ID in node_pipeline_relation
    rows = db_utils.get_rows_with_node_id_in_entries(cur, node_id)
    assert len(rows) == 1, "Expected one entry with the node ID."
    assert rows[0][3] == node_id, "Node ID in entry does not match expected value."
    assert rows[0][4] == pipeline_id, "Pipeline ID in entry does not match expected value."


def test_get_rows_node_id_in_nodes(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    conn.commit()

    # Get rows with the node ID in nodes
    rows = db_utils.get_rows_node_id_in_nodes(cur, node_id)
    assert len(rows) == 1, "Expected one row with the node ID."
    assert rows[0][0] == node_id, "Node ID in row does not match expected value."


def test_get_rows_with_node_id_in_node_tags(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add a tag to the node
    tag = "test_tag"
    db_utils.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Get rows with the node ID in tags
    rows = db_utils.get_rows_with_node_id_in_node_tags(cur, node_id)
    assert len(rows) == 1, "Expected one row with the node ID in tags."


def test_get_rows_with_node_id_relations(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
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

def test_get_rows_with_pipeline_id_in_entries(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a pipeline and nodes
    pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(2)]
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
    conn.commit()

    # Get rows with the pipeline ID in node_pipeline_relation
    rows = db_utils.get_rows_with_pipeline_id_in_entries(cur, pipeline_id)

    # Verify the rows match the expected data
    assert len(rows) == len(node_ids), f"Expected {len(node_ids)} rows, got {len(rows)}."
    for row in rows:
        assert row[4] == pipeline_id, f"Pipeline ID in row does not match expected value {pipeline_id}."
        assert row[3] in node_ids, f"Node ID {row[3]} in row is not in the expected node IDs {node_ids}."


def test_get_rows_with_pipeline_id_in_pipelines(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils


    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a pipeline
    pipeline_id = generate_pip_id()
    tag = "test_pipeline"
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Get rows with the pipeline ID in pipelines
    rows = db_utils.get_rows_with_pipeline_id_in_pipelines(cur, pipeline_id)

    # Verify the rows match the expected data
    assert len(rows) == 1, f"Expected one row, got {len(rows)}."
    assert rows[0][0] == pipeline_id, f"Pipeline ID in row does not match expected value {pipeline_id}."
    assert rows[0][1] == tag, f"Pipeline tag in row does not match expected value {tag}."



def test_duplicate_pipeline(in_memory_db_conn):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.db_utils import add_pipeline, add_node_to_pipeline, add_node_tag, duplicate_pipeline

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Add a source pipeline
    source_pipeline_id = "source_pipeline"
    add_pipeline( cur,
                  pipeline_id=source_pipeline_id,
                  tag="v1.0",
                  owner="group1",
                  notes="This is the source pipeline."
                )

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
    assert new_pipeline[2] == "group1"  # Owner should match the source pipeline
    assert new_pipeline[3] == "This is the source pipeline."  # Notes should match the source pipeline

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

    print("All assertions passed for test_duplicate_pipeline.")


def test_duplicate_pipeline_graph_comparison(in_memory_db_conn, dag_dummy_1):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import graph_to_db, db_to_graph_from_pip_id
    import networkx as nx

    # Setup database
    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Add the original graph to the database
    original_graph = dag_dummy_1
    graph_to_db(original_graph, cur)
    conn.commit()

    # Duplicate the pipeline
    original_pipeline_id = original_graph.graph['pipeline_id']
    new_pipeline_id = "duplicated_pipeline"
    db_utils.duplicate_pipeline(cur, original_pipeline_id, new_pipeline_id)
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
    assert original_graph_loaded.graph['pipeline_id'] != duplicated_graph_loaded.graph['pipeline_id'], \
        "Pipeline IDs should be different between the original and duplicated graphs."


def test_dupicate_node_in_pipeline_full_coverage(in_memory_db_conn):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create original node and another node (for relation)
    original_node_id = generate_node_id()
    other_node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=original_node_id)
    db_utils.add_node_to_nodes(cur, node_id=other_node_id)

    # Add a relation: original_node_id is child, other_node_id is parent
    db_utils.add_node_relation(cur, child_id=original_node_id, parent_id=other_node_id)
    # Add a relation: original_node_id is parent, other_node_id is child
    db_utils.add_node_relation(cur, child_id=other_node_id, parent_id=original_node_id)

    # Create a pipeline and add an entry connecting the pipeline to the node
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    user = "test_user"
    db_utils.add_node_to_pipeline(cur, node_id=original_node_id, pipeline_id=pipeline_id, user=user)
    conn.commit()

    # Add a tag to the original node
    tag = "test_tag"
    db_utils.add_node_tag(cur, node_id=original_node_id, pipeline_id=pipeline_id, tag=tag)

    # Duplicate the node
    duplicated_node_id = f"{original_node_id}_copy"
    db_utils.dupicate_node_in_pipeline(cur, original_node_id, duplicated_node_id, pipeline_id)
    conn.commit()

    # Check nodes table
    cur.execute("SELECT * FROM nodes WHERE node_id = ?", (duplicated_node_id,))
    duplicated_node = cur.fetchone()
    assert duplicated_node is not None, "Duplicated node was not created."

    # Check node_pipeline_relation table
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id = ? AND pipeline_id = ?", (duplicated_node_id, pipeline_id,))
    duplicated_entry = cur.fetchone()
    assert duplicated_entry is not None, "Duplicated node entry was not created."
    assert duplicated_entry[4] == pipeline_id, "Duplicated entry pipeline_id does not match original."


def test_copy_node_relations(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
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
    db_utils.copy_node_relations(cur, source_node_id, new_node_id)
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


def test_duplicate_node_in_pipeline_with_relations(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
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
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Duplicate node with relations
    db_utils.duplicate_node_in_pipeline_with_relations(cur, source_node_id, new_node_id, pipeline_id)
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


def test_remove_node_from_pipeline_removes_tags_and_entries(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Setup: create node, pipeline, entry, and tag
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
    db_utils.add_node_tag(cur, node_id=node_id, pipeline_id=pipeline_id, tag="test_tag")
    conn.commit()

    # Remove node from pipeline
    rows_deleted = db_utils.remove_node_from_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Check that the entry is removed from node_pipeline_relation
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    assert cur.fetchone() is None, "Entry was not removed from node_pipeline_relation table."

    # Check that the tag is removed from node_tags
    cur.execute("SELECT * FROM node_tags WHERE node_id=? AND pipeline_id=?", (node_id, pipeline_id))
    assert cur.fetchone() is None, "Tag was not removed from node_tags table."

    # Check that rowcount is at least 2 (one for node_pipeline_relation, one for node_tags)
    assert rows_deleted >= 1, "Expected at least one row to be deleted."


def test_replace_node_in_pipeline(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

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
        db_utils.add_node_to_nodes(cur, node_id=node)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")

    # Add relations: parent -> old_node -> child
    db_utils.add_node_relation(cur, child_id=old_node_id, parent_id=parent_id)
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=old_node_id)

    # Add entry and tag for old_node
    db_utils.add_node_to_pipeline(cur, node_id=old_node_id, pipeline_id=pipeline_id, user=user)
    db_utils.add_node_tag(cur, node_id=old_node_id, pipeline_id=pipeline_id, tag=tag)
    conn.commit()

    # Call the function under test
    result_new_node_id = db_utils.replace_node_in_pipeline(cur, old_node_id, new_node_id, pipeline_id)
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

    # Check old node is removed from node_pipeline_relation and tags for this pipeline
    cur.execute("SELECT * FROM node_pipeline_relation WHERE node_id=? AND pipeline_id=?", (old_node_id, pipeline_id))
    assert cur.fetchone() is None, "Old node entry was not removed from pipeline."

    cur.execute("SELECT * FROM node_tags WHERE node_id=? AND pipeline_id=?", (old_node_id, pipeline_id))
    assert cur.fetchone() is None, "Old node tag was not removed from pipeline."

    # The function should return the new node id
    assert result_new_node_id == new_node_id, "Returned new_node_id does not match expected value."


def test_get_pipelines_with_node(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node and multiple pipelines
    node_id = generate_node_id()
    pipeline_ids = [generate_pip_id() for _ in range(3)]
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    for pipeline_id in pipeline_ids:
        db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag=f"pipeline_{pipeline_id}")
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
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


def test_count_pipeline_with_node(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node and multiple pipelines
    node_id = generate_node_id()
    pipeline_ids = [generate_pip_id() for _ in range(3)]
    db_utils.add_node_to_nodes(cur, node_id=node_id)
    for pipeline_id in pipeline_ids:
        db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag=f"pipeline_{pipeline_id}")
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
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

def test_is_node_editable(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id)

    # Test case: Node does not belong to any pipeline
    editable = db_utils.is_node_editable(cur, node_id)
    assert editable is True, f"Expected node {node_id} to be editable when not associated with any pipeline."

    # Test case: Node belongs to one pipeline
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, user="test_user")
    conn.commit()

    editable = db_utils.is_node_editable(cur, node_id)
    assert editable is True, f"Expected node {node_id} to be editable when associated with one pipeline."

    # Test case: Node belongs to more than one pipeline
    another_pipeline_id = generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=another_pipeline_id, tag="another_test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=another_pipeline_id, user="test_user")
    conn.commit()

    editable = db_utils.is_node_editable(cur, node_id)
    assert editable is False, f"Expected node {node_id} to be non-editable when associated with more than one pipeline."


def test_update_editable_status_for_all_nodes(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
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
    db_utils.add_pipeline(cur, pipeline_id=pipeline1, tag="pipeline1")
    db_utils.add_pipeline(cur, pipeline_id=pipeline2, tag="pipeline2")

    # Associate nodes with pipelines
    db_utils.add_node_to_pipeline(cur, node_id=node1, pipeline_id=pipeline1, user="test_user")
    db_utils.add_node_to_pipeline(cur, node_id=node2, pipeline_id=pipeline1, user="test_user")
    db_utils.add_node_to_pipeline(cur, node_id=node2, pipeline_id=pipeline2, user="test_user")
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


def test_get_pipeline_notes_existing_and_nonexistent(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Add a pipeline with notes
    pipeline_id = generate_pip_id()
    notes = "This is a test note."
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline", notes=notes)
    conn.commit()

    # Test: get notes for existing pipeline
    fetched_notes = db_utils.get_pipeline_notes(cur, pipeline_id)
    assert fetched_notes == notes, f"Expected notes '{notes}', got '{fetched_notes}'"

    # Test: get notes for pipeline with no notes
    pipeline_id_no_notes = generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id_no_notes, tag="no_notes_pipeline")
    conn.commit()
    fetched_notes_none = db_utils.get_pipeline_notes(cur, pipeline_id_no_notes)
    assert fetched_notes_none is None, "Expected None for pipeline with no notes"

    # Test: get notes for non-existent pipeline
    non_existent_id = "nonexistent_id"
    assert db_utils.get_pipeline_notes(cur, non_existent_id) is None

def test_get_pipeline_owner_existing_and_nonexistent(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_pip_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Add a pipeline with an owner
    pipeline_id = generate_pip_id()
    owner = "test_owner"
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test_pipeline", owner=owner)
    conn.commit()

    # Test: get owner for existing pipeline
    fetched_owner = db_utils.get_pipeline_owner(cur, pipeline_id)
    assert fetched_owner == owner, f"Expected owner '{owner}', got '{fetched_owner}'"

    # Test: get owner for pipeline with no owner
    pipeline_id_no_owner = generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id_no_owner, tag="no_owner_pipeline")
    conn.commit()
    fetched_owner_none = db_utils.get_pipeline_owner(cur, pipeline_id_no_owner)
    assert fetched_owner_none is None, "Expected None for pipeline with no owner"

    # Test: get owner for non-existent pipeline
    non_existent_id = "nonexistent_id"
    assert db_utils.get_pipeline_owner(cur, non_existent_id) is None

def test_get_node_notes_existing_and_nonexistent(in_memory_db_conn):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Add a node with notes
    node_id = generate_node_id()
    notes = "This is a test node note."
    cur.execute("INSERT INTO nodes (node_id, notes) VALUES (?, ?)", (node_id, notes))
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
