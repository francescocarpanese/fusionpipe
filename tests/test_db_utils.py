import pytest
import tempfile
import os


def test_add_project_defaults_and_values(pg_test_db):
    from fusionpipe.utils import db_utils
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Test: Add project with all fields specified
    project_id = "proj_full"
    tag = "custom_tag"
    notes = "custom notes"
    owner = "custom_owner"
    returned_id = db_utils.add_project(cur, project_id=project_id, tag=tag, notes=notes, owner=owner)
    conn.commit()
    assert returned_id == project_id, "Returned project_id does not match input."

    cur.execute("SELECT project_id, tag, notes, owner FROM projects WHERE project_id=%s", (project_id,))
    result = cur.fetchone()
    assert result is not None, "Project not found in database."
    assert result[0] == project_id, "Project ID does not match."
    assert result[1] == tag, "Tag does not match."
    assert result[2] == notes, "Notes do not match."
    assert result[3] == owner, "Owner does not match."

    # Test: Add project with tag=None (should default to project_id)
    project_id2 = "proj_default_tag"
    returned_id2 = db_utils.add_project(cur, project_id=project_id2, tag=None, notes="notes2", owner="owner2")
    conn.commit()
    assert returned_id2 == project_id2, "Returned project_id does not match input for default tag."

    cur.execute("SELECT project_id, tag FROM projects WHERE project_id=%s", (project_id2,))
    result2 = cur.fetchone()
    assert result2 is not None, "Project with default tag not found."
    assert result2[1] == project_id2, "Tag was not set to project_id when tag=None."

    # Test: Add project with only required field (others None)
    project_id3 = "proj_minimal"
    returned_id3 = db_utils.add_project(cur, project_id=project_id3)
    conn.commit()
    assert returned_id3 == project_id3, "Returned project_id does not match input for minimal project."

    cur.execute("SELECT project_id, tag, notes, owner FROM projects WHERE project_id=%s", (project_id3,))
    result3 = cur.fetchone()
    assert result3 is not None, "Minimal project not found."
    assert result3[0] == project_id3, "Project ID does not match."
    assert result3[1] == project_id3, "Tag was not set to project_id for minimal project."
    assert result3[2] is None, "Notes should be None for minimal project."
    assert result3[3] is None, "Owner should be None for minimal project."

def test_add_pipeline(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_pip_id, generate_project_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = generate_project_id()
    db_utils.add_project(cur, project_id=project_id)

    pip_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(
        cur,
        pipeline_id=pip_id,
        project_id=project_id,    
        tag="test_pipeline",
        owner="test_user",
        notes="This is a test pipeline.",
        )
    
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
    assert result[4] == project_id, "Pipeline project_id does not match expected value."

def test_add_node(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_project_id

    # Initialize the PostgreSQL test database connection
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = generate_project_id()
    db_utils.add_project(cur, project_id=project_id)

    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    
    # Commit the transaction
    conn.commit()

    # Check if the node was added
    cur = conn.cursor()
    cur.execute("SELECT * FROM nodes WHERE node_id=%s", (node_id,))
    result = cur.fetchone()

    assert result[5] == node_id, "Node tag in entry does not match expected value (should be equal to node_id)."
    assert result[7] == project_id, "Project ID in entry does not match expected value."
    
    assert result is not None, f"Node {node_id} was not added to the database."


def test_add_node_to_pipline(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Generate a project
    project_id = generate_project_id()
    db_utils.add_project(cur, project_id=project_id)

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)

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
    from fusionpipe.utils.db_utils import remove_node_from_pipeline, get_rows_with_node_id_in_entries, get_rows_node_id_in_nodes, get_node_referenced_status
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Generate a project
    project_id = generate_project_id()
    db_utils.add_project(cur, project_id=project_id)

    # Create a node and two pipelines
    node_id = generate_node_id()
    pipeline_id1 = generate_pip_id()
    pipeline_id2 = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id1, project_id=project_id, tag="test_pipeline_1")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id2, project_id=project_id, tag="test_pipeline_2")
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

    # The node should be referenced only if it is present > 1 pipelines
    referenced = get_node_referenced_status(cur, node_id)
    cur.execute("SELECT COUNT(*) FROM node_pipeline_relation WHERE node_id=%s", (node_id,))
    count = cur.fetchone()[0]
    if count <= 1:
        assert referenced is False, "Node should not be referenced when present in one or zero pipelines."
    else:
        assert referenced is True, "Node should be referenced when present in more than one pipeline."

    # Now remove from the second pipeline as well
    rows_deleted2 = remove_node_from_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id2)
    conn.commit()

    # Node should still exist in nodes table
    assert db_utils.get_rows_node_id_in_nodes(cur, node_id), "Node was unexpectedly removed from nodes table after removing from all pipelines."

    # Node should now not be referenced (since it's not in any pipeline)
    referenced = get_node_referenced_status(cur, node_id)
    assert referenced is False, "Node should not be referenced when not present in any pipeline."



def test_add_node_relation(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create two nodes
    project_id = db_utils.add_project(cur, project_id="proj_for_relation")
    child_id = generate_node_id()
    parent_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=child_id, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=parent_id, project_id=project_id)

    # Add relation
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id, edge_id=f"000")
    conn.commit()

    # Check that only one relation exists and it matches the inserted values
    cur.execute("SELECT child_id, parent_id, edge_id FROM node_relation")
    results = cur.fetchall()
    assert len(results) == 1, f"Expected only one relation, found {len(results)}"
    assert results[0][0] == child_id, "Child ID in node relation does not match."
    assert results[0][1] == parent_id, "Parent ID in node relation does not match."
    assert results[0][2] == "000", "Edge ID in node relation does not match."

def test_get_node_parents(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create 3 nodes
    project_id = db_utils.add_project(cur, project_id="proj_for_parents")
    node1 = generate_node_id()
    node2 = generate_node_id()
    node3 = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node1, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node2, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node3, project_id=project_id)

    # Add relation: node1 is parent of node2
    db_utils.add_node_relation(cur, child_id=node2, parent_id=node1, edge_id="000")
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




def test_update_node_status(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, NodeState
    from fusionpipe.utils import db_utils


    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node
    project_id = db_utils.add_project(cur, project_id="proj_for_update_status")
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
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
    project_id = db_utils.add_project(cur, project_id="proj_for_pipeline_tag")
    pipeline_id = generate_pip_id()
    tag = "my_test_tag"
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=tag, project_id=project_id)
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
    project_id = db_utils.add_project(cur, project_id="proj_for_all_nodes_from_pip")
    pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(3)]
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()


    result_nodes = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)
    assert set(result_nodes) == set(node_ids), f"Expected nodes {node_ids}, got {result_nodes}"

    # Test with a pipeline that has no nodes
    empty_pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=empty_pipeline_id, tag="empty_pipeline", project_id=project_id)
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
    project_id = db_utils.add_project(cur, project_id="proj_for_nodes_without_pipeline")
    node_ids = [generate_node_id() for _ in range(3)]
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)

    # Create a pipeline and associate one node with it
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)
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
    project_id = db_utils.add_project(cur, project_id="proj_for_remove_relations")
    parent_id = generate_node_id()
    child_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=parent_id, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=child_id, project_id=project_id)

    # Add relation
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id, edge_id="000")
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
    project_id = db_utils.add_project(cur, project_id="proj_for_remove_everywhere")
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)

    # Add the node to node_pipeline_relation
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)

    # Add a relation involving the node
    parent_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=parent_id, project_id=project_id)
    db_utils.add_node_relation(cur, child_id=node_id, parent_id=parent_id, edge_id="000")
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
    project_id = db_utils.add_project(cur, project_id="proj_for_from_node_pipeline_relation")
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)

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
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Generate project and add to table
    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a node and a pipeline
    node_id = generate_node_id()
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)

    # Add the node to node_pipeline_relation
    entry_id = db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Get rows with the node ID in node_pipeline_relation
    rows = db_utils.get_rows_with_node_id_in_entries(cur, node_id)
    assert len(rows) == 1, "Expected one entry with the node ID."
    assert rows[0][0] == node_id, "Node ID in entry does not match expected value."
    assert rows[0][1] == pipeline_id, "Pipeline ID in entry does not match expected value."

def test_get_rows_node_id_in_nodes(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    conn.commit()

    # Get rows with the node ID in nodes
    rows = db_utils.get_rows_node_id_in_nodes(cur, node_id)
    assert len(rows) == 1, "Expected one row with the node ID."
    assert rows[0][0] == node_id, "Node ID in row does not match expected value."


def test_get_rows_with_node_id_relations(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_project_id

    from fusionpipe.utils.pip_utils import generate_node_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create two nodes and a relation
    parent_id = generate_node_id()
    child_id = generate_node_id()
    project_id = db_utils.add_project(cur, project_id=generate_project_id())
    db_utils.add_node_to_nodes(cur, node_id=parent_id, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=child_id, project_id=project_id)
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id, edge_id="000")
    conn.commit()

    # Get rows with the child node ID in relations
    rows = db_utils.get_rows_with_node_id_relations(cur, child_id)

    assert len(rows) == 1, "Expected one row with the child node ID in relations."

def test_get_rows_with_pipeline_id_in_node_pipeline(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a pipeline and nodes
    pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(2)]
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
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
    from fusionpipe.utils.pip_utils import generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils


    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a pipeline
    pipeline_id = generate_pip_id()
    tag = "test_pipeline"
    project_id = db_utils.add_project(cur, project_id=generate_project_id())
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=tag, project_id=project_id)
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

    project_id = db_utils.add_project(cur, project_id="proj_for_duplicate_pipeline")

    # Add a source pipeline
    source_pipeline_id = "source_pipeline"
    add_pipeline_to_pipelines( cur,
                  pipeline_id=source_pipeline_id,
                  project_id=project_id,
                  tag="v1.0",
                  owner="group1",
                  notes="This is the source pipeline."
                )

    # Add nodes and node_pipeline_relation to the source pipeline
    add_node_to_nodes(cur, node_id="node1", project_id=project_id, node_tag="node1_t")
    add_node_to_nodes(cur, node_id="node2", project_id=project_id, node_tag="node2_t")
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
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    projet_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create original node and another node (for relation)
    original_node_id = generate_node_id()
    other_node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=original_node_id, project_id=projet_id, node_tag="test_tag")
    db_utils.add_node_to_nodes(cur, node_id=other_node_id, project_id=projet_id, node_tag="other_tag")

    # Add a relation: original_node_id is child, other_node_id is parent
    db_utils.add_node_relation(cur, child_id=original_node_id, parent_id=other_node_id, edge_id="01")

    # Create a pipeline and add an entry connecting the pipeline to the node
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, project_id=projet_id, tag="test_pipeline")
    db_utils.add_node_to_pipeline(cur, node_id=original_node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Duplicate the node
    duplicated_node_id = generate_node_id()
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

    # Check that duplicated node belongs to the same project
    assert db_utils.get_project_id_by_node(cur, original_node_id) == db_utils.get_project_id_by_node(cur, duplicated_node_id)

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
    project_id = db_utils.add_project(cur, project_id="proj_for_copy_node_relations")
    source_node_id = generate_node_id()
    parent1_id = generate_node_id()
    parent2_id = generate_node_id()
    child1_id = generate_node_id()
    child2_id = generate_node_id()
    new_node_id = generate_node_id()

    # Add nodes to the database
    for node_id in [source_node_id, parent1_id, parent2_id, child1_id, child2_id, new_node_id]:
        db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)

    # Add parent relations (source_node_id is child of parent1 and parent2)
    db_utils.add_node_relation(cur, child_id=source_node_id, parent_id=parent1_id, edge_id='01')
    db_utils.add_node_relation(cur, child_id=source_node_id, parent_id=parent2_id, edge_id='02')

    # Add child relations (child1 and child2 are children of source_node_id)
    db_utils.add_node_relation(cur, child_id=child1_id, parent_id=source_node_id, edge_id='01')
    db_utils.add_node_relation(cur, child_id=child2_id, parent_id=source_node_id, edge_id='02')
    conn.commit()

    # Copy relations from source_node_id to new_node_id
    db_utils.copy_node_relations(cur, source_node_id, new_node_id, parents=parents, childrens=childrens)
    conn.commit()

    if parents:
        # Check parent relations for new_node_id (should match source_node_id's parents)
        cur.execute("SELECT parent_id, edge_id FROM node_relation WHERE child_id=%s", (new_node_id,))
        parent_rows = cur.fetchall()
        parent_ids = {row[0] for row in parent_rows}
        edge_ids = {row[1] for row in parent_rows}
        assert parent1_id in parent_ids and parent2_id in parent_ids, \
            f"Parent relations not copied correctly: {parent_ids}"
        # Check edge_ids match expected
        expected_edge_ids = {'01', '02'}
        assert edge_ids == expected_edge_ids, f"Parent edge_ids not copied correctly: {edge_ids}"

    if childrens:
        # Check child relations for new_node_id (should match source_node_id's children)
        cur.execute("SELECT child_id, edge_id FROM node_relation WHERE parent_id=%s", (new_node_id,))
        child_rows = cur.fetchall()
        child_ids = {row[0] for row in child_rows}
        edge_ids = {row[1] for row in child_rows}
        assert child1_id in child_ids and child2_id in child_ids, \
            f"Child relations not copied correctly: {child_ids}"
        # Check edge_ids match expected
        expected_edge_ids = {'01', '02'}
        assert edge_ids == expected_edge_ids, f"Child edge_ids not copied correctly: {edge_ids}"
   


def test_duplicate_node_in_pipeline_with_relations(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create nodes
    source_node_id = generate_node_id()
    parent_id = generate_node_id()
    child_id = generate_node_id()
    new_node_id = generate_node_id()

    # Add nodes to the database
    for node_id in [source_node_id, parent_id, child_id]:
        db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)

    # Add parent and child relations for the source node
    db_utils.add_node_relation(cur, child_id=source_node_id, parent_id=parent_id, edge_id='01')
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=source_node_id, edge_id='01')
    conn.commit()

    # Create a pipeline and add an entry connecting the pipeline to the source node
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, project_id=project_id, tag="test_pipeline")

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

    # Check duplicated nodes belongs to the same project
    assert db_utils.get_project_id_by_node(cur, node_id=source_node_id) == db_utils.get_project_id_by_node(cur, node_id=new_node_id)



def test_get_pipelines_with_node(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a node and multiple pipelines
    node_id = generate_node_id()
    pipeline_ids = [generate_pip_id() for _ in range(3)]
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    for pipeline_id in pipeline_ids:
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=f"pipeline_{pipeline_id}", project_id=project_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Call the function to get pipelines associated with the node
    pipelines_with_node = db_utils.get_pipelines_with_node(cur, node_id)

    # Verify the pipelines match the expected data
    assert set(pipelines_with_node) == set(pipeline_ids), f"Expected pipelines {pipeline_ids}, got {pipelines_with_node}"

    # Test with a node not associated with any pipeline
    new_node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=new_node_id, project_id=project_id)
    conn.commit()
    pipelines_with_new_node = db_utils.get_pipelines_with_node(cur, new_node_id)
    assert pipelines_with_new_node == [], f"Expected no pipelines for node {new_node_id}, got {pipelines_with_new_node}"


def test_count_pipeline_with_node(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a node and multiple pipelines
    node_id = generate_node_id()
    pipeline_ids = [generate_pip_id() for _ in range(3)]
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    for pipeline_id in pipeline_ids:
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=f"pipeline_{pipeline_id}", project_id=project_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Call the function to count pipelines associated with the node
    pipeline_count = db_utils.count_pipeline_with_node(cur, node_id)

    # Verify the count matches the expected value
    assert pipeline_count == len(pipeline_ids), f"Expected {len(pipeline_ids)} pipelines, got {pipeline_count}"

    # Test with a node not associated with any pipeline
    new_node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=new_node_id, project_id=project_id)
    conn.commit()
    new_node_pipeline_count = db_utils.count_pipeline_with_node(cur, new_node_id)
    assert new_node_pipeline_count == 0, f"Expected 0 pipelines for node {new_node_id}, got {new_node_pipeline_count}"

def test_get_node_referenced_status(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_id())

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)

    # Test case: Node does not belong to any pipeline
    referenced = db_utils.get_node_referenced_status(cur, node_id)
    assert referenced is False, f"Expected node {node_id} to not be referenced when not associated with any pipeline."

    # Test case: Node belongs to one pipeline
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    referenced = db_utils.get_node_referenced_status(cur, node_id)
    assert referenced is False, f"Expected node {node_id} to not be referenced when associated with only one pipeline."

    # Test case: Node belongs to more than one pipeline
    another_pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=another_pipeline_id, tag="another_test_pipeline", project_id=project_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=another_pipeline_id)
    conn.commit()

    referenced = db_utils.get_node_referenced_status(cur, node_id)
    assert referenced is True, f"Expected node {node_id} to be referenced when associated with more than one pipeline."


def test_update_referenced_status_for_all_nodes(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create nodes
    node1 = generate_node_id()
    node2 = generate_node_id()
    node3 = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node1, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node2, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node3, project_id=project_id)

    # Create pipelines
    pipeline1 = generate_pip_id()
    pipeline2 = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline1, tag="pipeline1", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline2, tag="pipeline2", project_id=project_id)

    # Associate nodes with pipelines
    db_utils.add_node_to_pipeline(cur, node_id=node1, pipeline_id=pipeline1)
    db_utils.add_node_to_pipeline(cur, node_id=node2, pipeline_id=pipeline1)
    db_utils.add_node_to_pipeline(cur, node_id=node2, pipeline_id=pipeline2)
    conn.commit()

    # Call the function to update referenced status
    db_utils.update_referenced_status_for_all_nodes(cur)
    conn.commit()

    # Verify referenced status for each node
    referenced_node1 = db_utils.get_node_referenced_status(cur, node1)
    assert referenced_node1 is False, f"Expected node1 to not be referenced, got {referenced_node1}"

    referenced_node2 =  db_utils.get_node_referenced_status(cur, node2)
    assert referenced_node2 is True, f"Expected node2 to be referenced, got {referenced_node2}"

    referenced_node3 = db_utils.get_node_referenced_status(cur, node3)
    assert referenced_node3 is False, f"Expected node3 to not be referenced, got {referenced_node3}"


def test_get_pipeline_notes_existing_and_nonexistent(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Add a pipeline with notes
    pipeline_id = generate_pip_id()
    notes = "This is a test note."
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", notes=notes, project_id=project_id)
    conn.commit()

    # Test: get notes for existing pipeline
    fetched_notes = db_utils.get_pipeline_notes(cur, pipeline_id)
    assert fetched_notes == notes, f"Expected notes '{notes}', got '{fetched_notes}'"

    # Test: get notes for pipeline with no notes
    pipeline_id_no_notes = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id_no_notes, tag="no_notes_pipeline", project_id=project_id)
    conn.commit()
    fetched_notes_none = db_utils.get_pipeline_notes(cur, pipeline_id_no_notes)
    assert fetched_notes_none is None, "Expected None for pipeline with no notes"

    # Test: get notes for non-existent pipeline
    non_existent_id = "nonexistent_id"
    assert db_utils.get_pipeline_notes(cur, non_existent_id) is None

def test_get_pipeline_owner_existing_and_nonexistent(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Add a pipeline with an owner
    pipeline_id = generate_pip_id()
    owner = "test_owner"
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", owner=owner, project_id=project_id)
    conn.commit()

    # Test: get owner for existing pipeline
    fetched_owner = db_utils.get_pipeline_owner(cur, pipeline_id)
    assert fetched_owner == owner, f"Expected owner '{owner}', got '{fetched_owner}'"

    # Test: get owner for pipeline with no owner
    pipeline_id_no_owner = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id_no_owner, tag="no_owner_pipeline", project_id=project_id)
    conn.commit()
    fetched_owner_none = db_utils.get_pipeline_owner(cur, pipeline_id_no_owner)
    assert fetched_owner_none is None, "Expected None for pipeline with no owner"

    # Test: get owner for non-existent pipeline
    non_existent_id = "nonexistent_id"
    assert db_utils.get_pipeline_owner(cur, non_existent_id) is None

def test_get_node_notes_existing_and_nonexistent(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

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
    db_utils.add_node_to_nodes(cur, node_id=node_id_no_notes, project_id=project_id)
    conn.commit()
    fetched_notes_none = db_utils.get_node_notes(cur, node_id_no_notes)
    assert fetched_notes_none is None, "Expected None for node with no notes"

    # Test: get notes for non-existent node
    non_existent_id = "nonexistent_node"
    assert db_utils.get_node_notes(cur, non_existent_id) is None


def test_remove_pipeline_removes_pipeline_from_everywhere(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils
    
    conn = pg_test_db
    cur = db_utils.init_db(conn)
    
    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a pipeline and related data
    pipeline_id = generate_pip_id()
    node_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", owner="owner", notes="notes", project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
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
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create nodes and pipeline
    node1 = 'n1'
    node2 = 'n2'
    node3 = 'n3'
    node4 = 'n4'
    node5 = 'n5'  # This node will be referenced
    node6 = 'n6'  # This node will be referenced
    node7 = 'n7'  # This node will be a child of node6
    pipeline_id = generate_pip_id()
    db_utils.add_node_to_nodes(cur, node_id=node1, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node2, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node3, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node4, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node5, referenced=True, project_id=project_id)  # referenced
    db_utils.add_node_to_nodes(cur, node_id=node6, referenced=True, project_id=project_id)  # referenced
    db_utils.add_node_to_nodes(cur, node_id=node7, project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)
    db_utils.add_node_to_pipeline(cur, node_id=node1, pipeline_id=pipeline_id)
    db_utils.add_node_to_pipeline(cur, node_id=node2, pipeline_id=pipeline_id)
    db_utils.add_node_to_pipeline(cur, node_id=node7, pipeline_id=pipeline_id)
    # node 1,2,3,7 are in the pipeline
    # node3, node4, node5, node6 are not in the pipeline
    # node5 and node6 are referenced
    # node6 is parent of node 7

    # Add relations:
    # node1 -> node2 (both in pipeline, should remain)
    db_utils.add_node_relation(cur, child_id=node2, parent_id=node1, edge_id="01")
    # node3 -> node4 (neither in pipeline, should remain untouched)
    db_utils.add_node_relation(cur, child_id=node4, parent_id=node3, edge_id="01")
    # node5 -> node6 (not in pipeline,  referenced, should be remain)
    db_utils.add_node_relation(cur, child_id=node6, parent_id=node5, edge_id="01")
    # node5 -> node7 (parents are  referenced and not in pipeline)
    db_utils.add_node_relation(cur, child_id=node7, parent_id=node5, edge_id="01")

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
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a pipeline and related data
    pipeline_id = generate_pip_id()
    node_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", owner="owner", notes="notes", project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
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

@pytest.mark.parametrize("node_status, blocked_status,referenced_status, expected_can_run", [
    ("ready", False, False, True),  # Node is ready and not blocked
    ("ready", True, False, False),  # Node is ready but blocked
    ("running", False, False,  False),  # Node is running
    ("failed", False, False, False),  # Node has failed
    ("completed", False, False, False),  # Node is completed
    ("ready", False, True, False),  # Node is ready but referenced
    ("running", False, True, False),  # Node is running and referenced
    ("failed", False, True, False),  # Node has failed and referenced
    ("completed", False, True, False),  # Node is completed and referenced
])
def test_can_node_run_logic(pg_test_db, node_status, blocked_status, referenced_status, expected_can_run):
    """
    Test logic for determining if a node can run based on its status.
    """
    from fusionpipe.utils import pip_utils
    from fusionpipe.utils import pip_utils
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a node with default status ('ready') and referenced False
    project_id = db_utils.add_project(cur, project_id="proj_for_relation")
    node_id = pip_utils.generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    conn.commit()
    canrun = pip_utils.can_node_run(cur, node_id)
    assert canrun is True, "Node should be able to run when status is 'ready' and referenced is False."

    # Update node status to 'running' and check again
    cur.execute("UPDATE nodes SET status=%s WHERE node_id=%s", (node_status, node_id,))
    cur.execute("UPDATE nodes SET blocked=%s WHERE node_id=%s", (blocked_status, node_id,))
    cur.execute("UPDATE nodes SET referenced=%s WHERE node_id=%s", (referenced_status, node_id,))
    conn.commit()
    canrun = pip_utils.can_node_run(cur, node_id)
    assert canrun is expected_can_run, "Node should not be able to run when status is 'running'."

def test_update_referenced_status(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a node
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    conn.commit()

    # Update referenced status to True
    rows_updated = db_utils.update_referenced_status(cur, node_id=node_id, referenced=True)
    conn.commit()

    # Verify the referenced status was updated
    assert rows_updated == 1, "Referenced status was not updated in the database."
    cur.execute("SELECT referenced FROM nodes WHERE node_id=%s", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found in database."
    assert result[0] == 1, f"Expected referenced status to be True, got {result[0]}"

    # Update referenced status back to False
    rows_updated = db_utils.update_referenced_status(cur, node_id=node_id, referenced=False)
    conn.commit()

    # Verify the referenced status was updated
    assert rows_updated == 1, "Referenced status was not updated in the database."
    cur.execute("SELECT referenced FROM nodes WHERE node_id=%s", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found in database."
    assert result[0] == 0, f"Expected referenced status to be False, got {result[0]}"


def test_duplicate_node_pipeline_relation(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create source pipeline and nodes
    source_pipeline_id = generate_pip_id()
    target_pipeline_id = generate_pip_id()
    node_ids = [generate_node_id() for _ in range(2)]
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=source_pipeline_id, tag="source_pipeline", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=target_pipeline_id, tag="target_pipeline", project_id=project_id)
    for node_id in node_ids:
        db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
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
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Add data to all tables
    pipeline_id = generate_pip_id()
    node_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    db_utils.add_node_relation(cur, child_id=node_id, parent_id=node_id, edge_id="01")
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
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)
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


def test_add_project_to_node(pg_test_db):
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add a project and a node
    project_id = "proj_456"
    node_id = generate_node_id()
    db_utils.add_project(cur, project_id=project_id, tag="test_project", notes="project notes", owner="owner2")
    db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id)
    conn.commit()

    # Verify association
    cur.execute("SELECT project_id FROM nodes WHERE node_id=%s", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found."
    assert result[0] == project_id, "Project ID not associated with node."




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
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pid, tag=f"pipeline_{pid}", project_id=project_id)
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
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pid, tag=f"pipeline_{pid}", project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id="node1", project_id=project_id)
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
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="pipeline_tag", project_id=project_id)
    conn.commit()

    # Should return the correct project_id
    result = db_utils.get_project_id_by_pipeline(cur, pipeline_id)
    assert result == project_id, f"Expected project_id '{project_id}', got '{result}'"

    # Pipeline with no project
    pipeline_id_no_project = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id_no_project, tag="no_project", project_id=None)
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

    project_id = db_utils.add_project(cur, project_id="proj_for_relation")

    # Add two pipelines
    parent_id = generate_pip_id()
    child_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_id, tag="parent_pipeline", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=child_id, tag="child_pipeline", project_id=project_id)
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

    project_id = db_utils.add_project(cur, project_id="proj_for_null_parent_relation")

    # Add a pipeline
    child_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=child_id, tag="child_pipeline", project_id=project_id)
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
    from fusionpipe.utils.pip_utils import generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Add pipelines
    parent_id1 = generate_pip_id()
    parent_id2 = generate_pip_id()
    child_id = generate_pip_id()
    unrelated_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_id1, tag="parent1", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_id2, tag="parent2", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=child_id, tag="child", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=unrelated_id, tag="unrelated", project_id=project_id)
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
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=root_pipeline_id, tag="root", project_id=project_id)
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
        db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pid, tag=f"tag_{pid}", project_id=project_id)
    conn.commit()

    # Add a pipeline not associated with the project
    unrelated_pipeline_id = "unrelated_pipeline"
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=unrelated_pipeline_id, tag="unrelated", project_id=None)
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


def test_is_pipeline_blocked(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a pipeline
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test_pipeline", project_id=project_id)
    conn.commit()

    # Test case: Pipeline with no nodes
    is_blocked = db_utils.is_pipeline_blocked(cur, pipeline_id)
    assert is_blocked is False, f"Expected pipeline {pipeline_id} to be non-blocked when it has no nodes."

    # Add a node to the pipeline
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, referenced=False, project_id=project_id)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Test case: Pipeline with one referenced node
    is_blocked = db_utils.is_pipeline_blocked(cur, pipeline_id)
    assert is_blocked is False, f"Expected pipeline {pipeline_id} to be non-blocked when it has at least one referenced node."

    raise RuntimeError("This is a remainder error to make the test fail intentionally." \
    "The logic for the blocking needs to be updated. Need to check that some operation are forbidden when node is blocked")

def test_get_node_blocked_status(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a node with blocked status set to False
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, blocked=False, project_id=project_id)
    conn.commit()

    # Test: Get blocked status for the node
    blocked_status = db_utils.get_node_blocked_status(cur, node_id)
    assert blocked_status is False, f"Expected blocked status to be False, got {blocked_status}"

    # Update the node's blocked status to True
    cur.execute("UPDATE nodes SET blocked = TRUE WHERE node_id = %s", (node_id,))
    conn.commit()

    # Test: Get updated blocked status for the node
    blocked_status = db_utils.get_node_blocked_status(cur, node_id)
    assert blocked_status is True, f"Expected blocked status to be True, got {blocked_status}"

    # Test: Get blocked status for a non-existent node
    non_existent_node_id = "nonexistent_node"
    blocked_status = db_utils.get_node_blocked_status(cur, non_existent_node_id)
    assert blocked_status is False, f"Expected blocked status to be False for non-existent node, got {blocked_status}"


def test_update_node_blocked_status(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_project_id
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create a node with blocked status set to False
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, blocked=False, project_id=project_id)
    conn.commit()

    # Test: Update blocked status to True
    rows_updated = db_utils.update_node_blocked_status(cur, node_id, True)
    conn.commit()
    assert rows_updated == 1, "Blocked status was not updated in the database."

    # Verify the blocked status was updated
    cur.execute("SELECT blocked FROM nodes WHERE node_id = %s", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found in database."
    assert result[0] is True, f"Expected blocked status to be True, got {result[0]}"

    # Test: Update blocked status back to False
    rows_updated = db_utils.update_node_blocked_status(cur, node_id, False)
    conn.commit()
    assert rows_updated == 1, "Blocked status was not updated in the database."

    # Verify the blocked status was updated
    cur.execute("SELECT blocked FROM nodes WHERE node_id = %s", (node_id,))
    result = cur.fetchone()
    assert result is not None, "Node not found in database."
    assert result[0] is False, f"Expected blocked status to be False, got {result[0]}"

    # Test: Update blocked status for a non-existent node
    non_existent_node_id = "nonexistent_node"
    rows_updated = db_utils.update_node_blocked_status(cur, non_existent_node_id, True)
    conn.commit()
    assert rows_updated == 0, "Blocked status should not be updated for a non-existent node."

    # Test: Pass invalid blocked status
    try:
        db_utils.update_node_blocked_status(cur, node_id, "invalid_status")
    except ValueError as e:
        assert str(e) == "blocked status must be a boolean value.", f"Unexpected error message: {str(e)}"
    else:
        assert False, "Expected ValueError for invalid blocked status, but no exception was raised."

def test_remove_all_pipeline_relation_of_pipeline_id(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_pip_id, generate_project_id
    import fusionpipe.utils.db_utils as db_utils
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Add pipelines
    pipeline_id = generate_pip_id()
    parent_id1 = generate_pip_id()
    parent_id2 = generate_pip_id()
    child_id1 = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="main", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_id1, tag="parent1", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_id2, tag="parent2", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=child_id1, tag="child1", project_id=project_id)
    conn.commit()

    # Add relations where pipeline_id is both parent and child
    db_utils.add_pipeline_relation(cur, child_id=pipeline_id, parent_id=parent_id1)
    db_utils.add_pipeline_relation(cur, child_id=child_id1, parent_id=pipeline_id)
    db_utils.add_pipeline_relation(cur, child_id=pipeline_id, parent_id=parent_id2)
    conn.commit()

    # There should be 3 relations involving pipeline_id
    cur.execute("SELECT COUNT(*) FROM pipeline_relation WHERE child_id=%s OR parent_id=%s", (pipeline_id, pipeline_id))
    count_before = cur.fetchone()[0]
    assert count_before == 3, f"Expected 3 relations before removal, got {count_before}"

    # Remove all relations for pipeline_id
    rows_deleted = db_utils.remove_all_pipeline_relation_of_pipeline_id(cur, pipeline_id)
    conn.commit()
    assert rows_deleted == 3, f"Expected 3 rows deleted, got {rows_deleted}"

    # Verify all relations involving pipeline_id are removed
    cur.execute("SELECT * FROM pipeline_relation WHERE child_id=%s OR parent_id=%s", (pipeline_id, pipeline_id))
    assert cur.fetchone() is None, "Relations involving pipeline_id were not fully removed."

    # Other relations should remain unaffected (add one unrelated and check)
    unrelated_child = generate_pip_id()
    unrelated_parent = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=unrelated_child, tag="unrelated_child", project_id=project_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=unrelated_parent, tag="unrelated_parent", project_id=project_id)
    db_utils.add_pipeline_relation(cur, child_id=unrelated_child, parent_id=unrelated_parent)
    conn.commit()
    cur.execute("SELECT * FROM pipeline_relation WHERE child_id=%s AND parent_id=%s", (unrelated_child, unrelated_parent))
    assert cur.fetchone() is not None, "Unrelated pipeline relation was incorrectly removed."

def test_get_node_relation_edge_id(pg_test_db):
    from fusionpipe.utils.pip_utils import generate_node_id, generate_project_id
    import fusionpipe.utils.db_utils as db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create parent and child nodes
    parent_id = generate_node_id()
    child_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=parent_id, project_id=project_id)
    db_utils.add_node_to_nodes(cur, node_id=child_id, project_id=project_id)

    # Add a node relation with a specific edge_id
    edge_id = "edge_123"
    db_utils.add_node_relation(cur, child_id, parent_id, edge_id)
    conn.commit()

    # Test: get_node_relation_edge_id returns the correct edge_id
    result = db_utils.get_node_relation_edge_id(cur, child_id, parent_id)
    assert result == edge_id, f"Expected edge_id '{edge_id}', got '{result}'"

    # Test: get_node_relation_edge_id returns None for non-existent relation
    non_existent_child = generate_node_id()
    non_existent_parent = generate_node_id()
    result_none = db_utils.get_node_relation_edge_id(cur, non_existent_child, non_existent_parent)
    assert result_none is None, "Expected None for non-existent node relation"

def test_get_edge_id_of_all_node_parents(pg_test_db):
    import fusionpipe.utils.db_utils as db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_project_id

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    project_id = db_utils.add_project(cur, project_id=generate_project_id())

    # Create child node and multiple parent nodes
    child_id = generate_node_id()
    parent_ids = [generate_node_id() for _ in range(3)]
    edge_ids = [f"edge_{i}" for i in range(3)]
    db_utils.add_node_to_nodes(cur, node_id=child_id, project_id=project_id)
    for pid in parent_ids:
        db_utils.add_node_to_nodes(cur, node_id=pid, project_id=project_id)
    # Add relations with specific edge_ids
    for pid, eid in zip(parent_ids, edge_ids):
        db_utils.add_node_relation(cur, child_id, pid, eid)
    conn.commit()

    # Test: get_edge_id_of_all_node_parents returns all edge_ids for the child
    result = db_utils.get_edge_id_of_all_node_parents(cur, child_id)
    assert set(result) == set(edge_ids), f"Expected edge_ids {edge_ids}, got {result}"

    # Test: returns empty list for node with no parents
    orphan_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=orphan_id, project_id=project_id)
    conn.commit()
    result_empty = db_utils.get_edge_id_of_all_node_parents(cur, orphan_id)
    assert result_empty == [], f"Expected empty list for node with no parents, got {result_empty}"



def test_drop_all_tables(pg_test_db):
    from fusionpipe.utils import db_utils
    import psycopg2

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add some data to ensure tables exist
    db_utils.add_project(cur, project_id="proj_drop", tag="tag", notes="notes", owner="owner")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id="pipeline_drop", tag="tag", project_id="proj_drop")
    conn.commit()

    # Drop all tables
    db_utils.drop_all_tables(conn)

    # After dropping, querying any table should raise an error
    cur = conn.cursor()
    with pytest.raises(psycopg2.errors.UndefinedTable):
        cur.execute("SELECT * FROM projects")
    conn.rollback()  # Reset transaction state after error
    with pytest.raises(psycopg2.errors.UndefinedTable):
        cur.execute("SELECT * FROM pipelines")
    conn.rollback()  # Reset transaction state after error
    with pytest.raises(psycopg2.errors.UndefinedTable):
        cur.execute("SELECT * FROM nodes")
    conn.rollback()  # Reset transaction state after error
    with pytest.raises(psycopg2.errors.UndefinedTable):
        cur.execute("SELECT * FROM node_pipeline_relation")
    conn.rollback()  # Reset transaction state after error
    with pytest.raises(psycopg2.errors.UndefinedTable):
        cur.execute("SELECT * FROM node_relation")
    conn.rollback()  # Reset transaction state after error
    with pytest.raises(psycopg2.errors.UndefinedTable):
        cur.execute("SELECT * FROM processes")
    conn.rollback()  # Reset transaction state after error


# ---------------------------------------------------------------------------
# Node group tests
# ---------------------------------------------------------------------------

def _setup_pipeline_with_nodes(cur, conn, n_nodes=2):
    """Helper: create a project, pipeline, and n_nodes; return (pipeline_id, node_ids)."""
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_pip_id, generate_project_id, generate_node_id, generate_id

    project_id = generate_project_id()
    db_utils.add_project(cur, project_id=project_id)
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag=generate_id(), project_id=project_id)
    node_ids = []
    for _ in range(n_nodes):
        node_id = generate_node_id()
        db_utils.add_node_to_nodes(cur, node_id=node_id, project_id=project_id, folder_path=f"/tmp/{node_id}")
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
        node_ids.append(node_id)
    conn.commit()
    return pipeline_id, node_ids


def test_create_node_group(pg_test_db):
    """create_node_group inserts a row in node_groups with the expected values."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, _ = _setup_pipeline_with_nodes(cur, conn)

    group_id = "ng_test1"
    db_utils.create_node_group(
        cur, group_id, pipeline_id,
        tag="my group", collapsed=False,
        pos_x=10.0, pos_y=20.0, width=300.0, height=200.0,
    )
    conn.commit()

    cur.execute(
        "SELECT group_id, pipeline_id, tag, collapsed, pos_x, pos_y, width, height "
        "FROM node_groups WHERE group_id = %s",
        (group_id,),
    )
    row = cur.fetchone()
    assert row is not None, "Node group row not found."
    assert row[0] == group_id
    assert row[1] == pipeline_id
    assert row[2] == "my group"
    assert row[3] is False
    assert float(row[4]) == 10.0
    assert float(row[5]) == 20.0
    assert float(row[6]) == 300.0
    assert float(row[7]) == 200.0


def test_create_node_group_defaults(pg_test_db):
    """create_node_group uses sensible defaults for optional parameters."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, _ = _setup_pipeline_with_nodes(cur, conn)

    group_id = "ng_defaults"
    db_utils.create_node_group(cur, group_id, pipeline_id)
    conn.commit()

    cur.execute(
        "SELECT collapsed, pos_x, pos_y, width, height FROM node_groups WHERE group_id = %s",
        (group_id,),
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] is False          # collapsed default
    assert float(row[1]) == 0.0    # pos_x default
    assert float(row[2]) == 0.0    # pos_y default
    assert float(row[3]) == 400.0  # width default
    assert float(row[4]) == 300.0  # height default


def test_add_node_to_group_relation(pg_test_db):
    """add_node_to_group_relation creates a membership row for each node."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, node_ids = _setup_pipeline_with_nodes(cur, conn, n_nodes=3)

    group_id = "ng_members"
    db_utils.create_node_group(cur, group_id, pipeline_id)
    for nid in node_ids:
        db_utils.add_node_to_group_relation(cur, nid, group_id)
    conn.commit()

    cur.execute(
        "SELECT node_id FROM node_group_relation WHERE group_id = %s ORDER BY node_id",
        (group_id,),
    )
    db_members = {row[0] for row in cur.fetchall()}
    assert db_members == set(node_ids)


def test_add_node_to_group_relation_upsert(pg_test_db):
    """Assigning a node already in one group to another group (upsert) works correctly."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, node_ids = _setup_pipeline_with_nodes(cur, conn, n_nodes=1)
    node_id = node_ids[0]

    group_a = "ng_upsert_a"
    group_b = "ng_upsert_b"
    db_utils.create_node_group(cur, group_a, pipeline_id)
    db_utils.create_node_group(cur, group_b, pipeline_id)

    # Assign to group A first
    db_utils.add_node_to_group_relation(cur, node_id, group_a)
    conn.commit()
    assert db_utils.get_group_for_node(cur, node_id) == group_a

    # Re-assign to group B via upsert
    db_utils.add_node_to_group_relation(cur, node_id, group_b)
    conn.commit()
    assert db_utils.get_group_for_node(cur, node_id) == group_b

    # Only one row should exist for this node
    cur.execute("SELECT COUNT(*) FROM node_group_relation WHERE node_id = %s", (node_id,))
    assert cur.fetchone()[0] == 1


def test_delete_node_group(pg_test_db):
    """delete_node_group removes the group row and all membership rows."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, node_ids = _setup_pipeline_with_nodes(cur, conn, n_nodes=2)

    group_id = "ng_del"
    db_utils.create_node_group(cur, group_id, pipeline_id)
    for nid in node_ids:
        db_utils.add_node_to_group_relation(cur, nid, group_id)
    conn.commit()

    db_utils.delete_node_group(cur, group_id)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM node_groups WHERE group_id = %s", (group_id,))
    assert cur.fetchone()[0] == 0, "Group row should be gone."

    cur.execute("SELECT COUNT(*) FROM node_group_relation WHERE group_id = %s", (group_id,))
    assert cur.fetchone()[0] == 0, "All membership rows should be gone."

    # Nodes themselves must still exist
    for nid in node_ids:
        cur.execute("SELECT COUNT(*) FROM nodes WHERE node_id = %s", (nid,))
        assert cur.fetchone()[0] == 1, f"Node {nid} should not be deleted."


def test_get_groups_for_pipeline(pg_test_db):
    """get_groups_for_pipeline returns the correct structure for all groups in a pipeline."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, node_ids = _setup_pipeline_with_nodes(cur, conn, n_nodes=4)

    # Create two groups on the same pipeline
    g1, g2 = "ng_gfp_1", "ng_gfp_2"
    db_utils.create_node_group(cur, g1, pipeline_id, tag="alpha", pos_x=1.0, pos_y=2.0, width=100.0, height=50.0)
    db_utils.create_node_group(cur, g2, pipeline_id, tag="beta", collapsed=True, pos_x=5.0, pos_y=6.0, width=200.0, height=80.0)
    db_utils.add_node_to_group_relation(cur, node_ids[0], g1)
    db_utils.add_node_to_group_relation(cur, node_ids[1], g1)
    db_utils.add_node_to_group_relation(cur, node_ids[2], g2)
    conn.commit()

    groups = db_utils.get_groups_for_pipeline(cur, pipeline_id)
    assert len(groups) == 2

    by_id = {g["group_id"]: g for g in groups}

    assert by_id[g1]["tag"] == "alpha"
    assert by_id[g1]["collapsed"] is False
    assert by_id[g1]["pos_x"] == 1.0
    assert by_id[g1]["pos_y"] == 2.0
    assert by_id[g1]["width"] == 100.0
    assert by_id[g1]["height"] == 50.0
    assert set(by_id[g1]["node_ids"]) == {node_ids[0], node_ids[1]}

    assert by_id[g2]["tag"] == "beta"
    assert by_id[g2]["collapsed"] is True
    assert set(by_id[g2]["node_ids"]) == {node_ids[2]}

    # node_ids[3] belongs to no group and must not appear in any node_ids list
    all_member_ids = by_id[g1]["node_ids"] + by_id[g2]["node_ids"]
    assert node_ids[3] not in all_member_ids


def test_get_groups_for_pipeline_no_groups(pg_test_db):
    """get_groups_for_pipeline returns an empty list when no groups exist."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, _ = _setup_pipeline_with_nodes(cur, conn)

    groups = db_utils.get_groups_for_pipeline(cur, pipeline_id)
    assert groups == []


def test_update_node_group_collapse(pg_test_db):
    """update_node_group_collapse persists the collapsed flag correctly."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, _ = _setup_pipeline_with_nodes(cur, conn)

    group_id = "ng_collapse"
    db_utils.create_node_group(cur, group_id, pipeline_id, collapsed=False)
    conn.commit()

    # Collapse the group
    db_utils.update_node_group_collapse(cur, group_id, True)
    conn.commit()
    cur.execute("SELECT collapsed FROM node_groups WHERE group_id = %s", (group_id,))
    assert cur.fetchone()[0] is True

    # Expand again
    db_utils.update_node_group_collapse(cur, group_id, False)
    conn.commit()
    cur.execute("SELECT collapsed FROM node_groups WHERE group_id = %s", (group_id,))
    assert cur.fetchone()[0] is False


def test_update_node_group_position(pg_test_db):
    """update_node_group_position persists pos_x, pos_y, width, height."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, _ = _setup_pipeline_with_nodes(cur, conn)

    group_id = "ng_pos"
    db_utils.create_node_group(cur, group_id, pipeline_id)
    conn.commit()

    db_utils.update_node_group_position(cur, group_id, 50.5, 75.0, 500.0, 400.0)
    conn.commit()

    cur.execute(
        "SELECT pos_x, pos_y, width, height FROM node_groups WHERE group_id = %s",
        (group_id,),
    )
    row = cur.fetchone()
    assert float(row[0]) == 50.5
    assert float(row[1]) == 75.0
    assert float(row[2]) == 500.0
    assert float(row[3]) == 400.0


def test_update_node_group_tag(pg_test_db):
    """update_node_group_tag changes the human-readable label."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, _ = _setup_pipeline_with_nodes(cur, conn)

    group_id = "ng_tag"
    db_utils.create_node_group(cur, group_id, pipeline_id, tag="original")
    conn.commit()

    db_utils.update_node_group_tag(cur, group_id, "renamed")
    conn.commit()

    cur.execute("SELECT tag FROM node_groups WHERE group_id = %s", (group_id,))
    assert cur.fetchone()[0] == "renamed"


def test_get_group_for_node(pg_test_db):
    """get_group_for_node returns the correct group_id or None."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, node_ids = _setup_pipeline_with_nodes(cur, conn, n_nodes=2)
    grouped_node, ungrouped_node = node_ids

    group_id = "ng_gfn"
    db_utils.create_node_group(cur, group_id, pipeline_id)
    db_utils.add_node_to_group_relation(cur, grouped_node, group_id)
    conn.commit()

    assert db_utils.get_group_for_node(cur, grouped_node) == group_id
    assert db_utils.get_group_for_node(cur, ungrouped_node) is None


def test_delete_groups_for_pipeline(pg_test_db):
    """delete_groups_for_pipeline removes all groups and memberships for a pipeline."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, node_ids = _setup_pipeline_with_nodes(cur, conn, n_nodes=3)

    # Create two groups on the target pipeline and one on a different pipeline
    pipeline_id2, other_nodes = _setup_pipeline_with_nodes(cur, conn, n_nodes=1)

    g1, g2, g_other = "ng_dp_1", "ng_dp_2", "ng_dp_other"
    db_utils.create_node_group(cur, g1, pipeline_id)
    db_utils.create_node_group(cur, g2, pipeline_id)
    db_utils.create_node_group(cur, g_other, pipeline_id2)

    db_utils.add_node_to_group_relation(cur, node_ids[0], g1)
    db_utils.add_node_to_group_relation(cur, node_ids[1], g2)
    db_utils.add_node_to_group_relation(cur, other_nodes[0], g_other)
    conn.commit()

    db_utils.delete_groups_for_pipeline(cur, pipeline_id)
    conn.commit()

    # All groups on pipeline_id should be gone
    cur.execute("SELECT COUNT(*) FROM node_groups WHERE pipeline_id = %s", (pipeline_id,))
    assert cur.fetchone()[0] == 0

    # Memberships of those groups should be gone
    cur.execute("SELECT COUNT(*) FROM node_group_relation WHERE group_id IN (%s, %s)", (g1, g2))
    assert cur.fetchone()[0] == 0

    # Group on the other pipeline must be intact
    cur.execute("SELECT COUNT(*) FROM node_groups WHERE group_id = %s", (g_other,))
    assert cur.fetchone()[0] == 1


def test_remove_node_from_everywhere_removes_from_group(pg_test_db):
    """remove_node_from_everywhere also cleans up node_group_relation for that node."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, node_ids = _setup_pipeline_with_nodes(cur, conn, n_nodes=2)
    node_to_delete, surviving_node = node_ids

    group_id = "ng_rnfe"
    db_utils.create_node_group(cur, group_id, pipeline_id)
    db_utils.add_node_to_group_relation(cur, node_to_delete, group_id)
    db_utils.add_node_to_group_relation(cur, surviving_node, group_id)
    conn.commit()

    db_utils.remove_node_from_everywhere(cur, node_to_delete)
    conn.commit()

    # The deleted node's membership must be gone
    cur.execute(
        "SELECT COUNT(*) FROM node_group_relation WHERE node_id = %s", (node_to_delete,)
    )
    assert cur.fetchone()[0] == 0

    # The surviving node's membership should remain
    assert db_utils.get_group_for_node(cur, surviving_node) == group_id

    # The group itself must still exist
    cur.execute("SELECT COUNT(*) FROM node_groups WHERE group_id = %s", (group_id,))
    assert cur.fetchone()[0] == 1


def test_remove_pipeline_from_everywhere_removes_groups(pg_test_db):
    """remove_pipeline_from_everywhere also removes all node groups for the pipeline."""
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id, node_ids = _setup_pipeline_with_nodes(cur, conn, n_nodes=2)

    group_id = "ng_rpfe"
    db_utils.create_node_group(cur, group_id, pipeline_id)
    for nid in node_ids:
        db_utils.add_node_to_group_relation(cur, nid, group_id)
    conn.commit()

    db_utils.remove_pipeline_from_everywhere(cur, pipeline_id)
    conn.commit()

    # Group and memberships must be gone
    cur.execute("SELECT COUNT(*) FROM node_groups WHERE group_id = %s", (group_id,))
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT COUNT(*) FROM node_group_relation WHERE group_id = %s", (group_id,))
    assert cur.fetchone()[0] == 0
