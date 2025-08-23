import pytest
import tempfile
import os
import sqlite3


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

def test_init_node_folder(tmp_base_dir, monkeypatch):
    # Test if the function creates the expected node folder structure and initializes the code folder
    from fusionpipe.utils.pip_utils import init_node_folder
    import os
    import toml

    folder_path_nodes = os.path.join(tmp_base_dir, "test_node")
    init_node_folder(folder_path_nodes, verbose=True)

    code_folder = os.path.join(folder_path_nodes, "code")
    data_folder = os.path.join(folder_path_nodes, "data")
    reports_folder = os.path.join(folder_path_nodes, "reports")

    assert os.path.isdir(folder_path_nodes), "Node folder was not created."
    assert os.path.isdir(code_folder), "Code subfolder was not created."
    assert os.path.isdir(data_folder), "Data subfolder was not created."
    assert os.path.isdir(reports_folder), "Reports subfolder was not created."

    # Check if the main.py file was created in the code folder
    main_file_path = os.path.join(code_folder, "main.py")
    assert os.path.isfile(main_file_path), "main.py file was not created in the code folder."

    # Check if pyproject.toml file was updated
    pyproject_file_path = os.path.join(code_folder, "pyproject.toml")
    assert os.path.isfile(pyproject_file_path), "pyproject.toml file was not created in the code folder."
    with open(pyproject_file_path, "r") as f:
        pyproject_content = toml.load(f)
    assert "test_node" in pyproject_content["project"]["name"], "pyproject.toml does not contain 'dependencies' section."


def test_delete_node_folder_removes_existing_folder(tmp_base_dir):
    # Test if the function deletes an existing node folder
    from fusionpipe.utils.pip_utils import delete_node_folder
    import os

    node_id = "testnode"
    node_folder_path = os.path.join(tmp_base_dir, f"n_{node_id}")
    os.makedirs(node_folder_path, exist_ok=True)
    # Ensure the folder exists before deletion
    assert os.path.exists(node_folder_path)
    delete_node_folder(node_folder_path, verbose=True)
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
    delete_node_folder(node_folder_path, verbose=True)
    # Should not raise, and should print a message
    captured = capsys.readouterr()
    assert f"Node folder does not exist" in captured.out


def test_pipeline_graph_to_dict(dag_dummy_1, dict_dummy_1):
    from fusionpipe.utils.pip_utils import pipeline_graph_to_dict
    import networkx as nx

    # Convert the graph to a dictionary
    graph_dict = pipeline_graph_to_dict(dag_dummy_1)

    # Check if the converted dictionary matches the expected dictionary
    assert graph_dict == dict_dummy_1, "Graph to dict conversion did not produce the expected result."


def test_project_graph_to_dict(dag_dummy_project, dict_dummy_project):
    from fusionpipe.utils.pip_utils import project_graph_to_dict
    import networkx as nx

    # Convert the graph to a dictionary
    graph_dict = project_graph_to_dict(dag_dummy_project)

    # Check if the converted dictionary matches the expected dictionary
    assert graph_dict == dict_dummy_project, "Graph to dict conversion did not produce the expected result."


def test_graph_dict_to_json(tmp_base_dir, dict_dummy_1):
    from fusionpipe.utils.pip_utils import graph_dict_to_json
    import json

    # Prepare file path
    file_path = os.path.join(tmp_base_dir, "test_pipeline.json")
    # Write the dictionary to JSON
    graph_dict_to_json(dict_dummy_1, file_path, verbose=True)

    # Check file exists
    assert os.path.exists(file_path), "JSON file was not created."

    # Check file content matches the dictionary
    with open(file_path, "r") as f:
        loaded = json.load(f)
    assert loaded == dict_dummy_1, "JSON file content does not match the original dictionary."


def test_pipeline_graph_to_db_and_db_to_pipeline_graph_roundtrip(pg_test_db, dag_dummy_1):
    """
    Test that a graph can be written to the database and then read back,
    and that the structure and attributes are preserved.
    """
    from fusionpipe.utils.pip_utils import pipeline_graph_to_db, db_to_pipeline_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx

    # Setup database
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Write the dummy graph to the database
    pipeline_graph_to_db(dag_dummy_1, cur)
    conn.commit()

    # Read the graph back from the database
    G_loaded = db_to_pipeline_graph_from_pip_id(cur, dag_dummy_1.graph['pipeline_id'])

    # Use networkx's is_isomorphic to compare structure and attributes
    def node_match(n1, n2):
        # Compare relevant node attributes
        for attr in ['status', 'referenced', 'tag', 'notes', 'folder_path', 'blocked', 'edge_id']:
            if n1.get(attr) != n2.get(attr):
                return False
        return True

    def edge_match(e1, e2):
        # No edge attributes to compare, just return True
        return True

    assert nx.is_isomorphic(
        G_loaded, dag_dummy_1,
        node_match=node_match,
        edge_match=edge_match
    ), "Loaded graph is not isomorphic to the original graph with respect to structure and node attributes."

    # Check graph attributes
    for attr in ['pipeline_id', 'notes', 'tag', 'owner', 'blocked']:
        assert G_loaded.graph[attr] == dag_dummy_1.graph[attr]


def test_project_graph_to_db_and_db_to_project_graph_roundtrip(pg_test_db, dag_dummy_project):
    """
    Test that a graph can be written to the database and then read back,
    and that the structure and attributes are preserved.
    """
    from fusionpipe.utils.pip_utils import project_graph_to_db, db_to_project_graph_from_project_id
    from fusionpipe.utils import db_utils
    import networkx as nx

    # Setup database
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Write the dummy graph to the database
    project_graph_to_db(dag_dummy_project, cur)
    conn.commit()

    # Read the graph back from the database
    G_loaded = db_to_project_graph_from_project_id(cur, dag_dummy_project.graph['project_id'])

    # Use networkx's is_isomorphic to compare structure and attributes
    def node_match(n1, n2):
        # Compare relevant node attributes
        for attr in ['tag', 'notes']:
            if n1.get(attr) != n2.get(attr):
                return False
        return True

    def edge_match(e1, e2):
        # No edge attributes to compare, just return True
        return True

    assert nx.is_isomorphic(
        G_loaded, dag_dummy_project,
        node_match=node_match,
        edge_match=edge_match
    ), "Loaded graph is not isomorphic to the original graph with respect to structure and node attributes."

    # Check graph attributes
    for attr in ['project_id', 'notes', 'tag', 'owner']:
        assert G_loaded.graph[attr] == dag_dummy_project.graph[attr]


def test_dict_to_graph(dict_dummy_1, dag_dummy_1):
    """
    Test that dict_to_graph correctly reconstructs a NetworkX graph from a dictionary,
    preserving structure and node/graph attributes.
    """
    from fusionpipe.utils.pip_utils import dict_to_graph
    import networkx as nx

    # Convert the dictionary to a graph
    G = dict_to_graph(dict_dummy_1)

    # Use networkx's is_isomorphic to compare structure and attributes
    def node_match(n1, n2):
        for attr in ['status', 'referenced', 'tag', 'notes', 'blocked']:
            if n1.get(attr) != n2.get(attr):
                return False
        return True

    def edge_match(e1, e2):
        return True

    assert nx.is_isomorphic(
        G, dag_dummy_1,
        node_match=node_match,
        edge_match=edge_match
    ), "Graph reconstructed from dict is not isomorphic to the original graph."

    # Check graph attributes
    for attr in ['pipeline_id', 'notes', 'tag', 'owner','blocked']:
        assert G.graph[attr] == dag_dummy_1.graph[attr]

def test_visualize_pip_static_runs_without_error(monkeypatch, dag_dummy_1):
    """
    Test that visualize_pip_static runs without raising exceptions.
    We patch plt.show to avoid opening a window during tests.
    """

    # Patch plt.show to a no-op
    import matplotlib.pyplot as plt
    from fusionpipe.utils.pip_utils import visualize_pip_static
    monkeypatch.setattr(plt, "show", lambda: None)

    # Should not raise
    visualize_pip_static(dag_dummy_1)

def test_get_all_children_nodes(pg_test_db, dag_dummy_1):
    """
    Test that get_all_descendants returns all descendants of a node in the pipeline.
    """
    from fusionpipe.utils.pip_utils import get_all_descendants, pipeline_graph_to_db
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    pipeline_graph_to_db(dag_dummy_1, cur)
    conn.commit()

    # For each node, compare get_all_descendants to nx.descendants
    for node in dag_dummy_1.nodes:
        expected = set(nx.descendants(dag_dummy_1, node))
        result = set(get_all_descendants(cur, dag_dummy_1.graph['pipeline_id'], node))
        assert result == expected, f"Children nodes for {node} do not match expected descendants."

from conftest import PARENT_NODE_LIST
@pytest.mark.parametrize("start_node", PARENT_NODE_LIST)
def test_branch_pipeline_from_node(monkeypatch, pg_test_db, dag_dummy_1, start_node, tmp_base_dir):
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)
    """
    Test that branch_pipeline_from_node creates a new pipeline where all nodes are preserved,
    except the provided node and all its descendants, which are replaced with new IDs.
    """
    from fusionpipe.utils.pip_utils import branch_pipeline_from_node, db_to_pipeline_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx
    from fusionpipe.utils.pip_utils import pipeline_graph_to_db    

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the original graph to the database
    original_graph = dag_dummy_1

    pipeline_graph_to_db(original_graph, cur)
    conn.commit()

    # Get all pipeline IDs before
    pipeline_ids_before = set(db_utils.get_all_pipeline_ids(cur))

    # Run branch_pipeline_from_node
    branch_pipeline_from_node(cur, original_graph.graph['pipeline_id'], start_node)
    conn.commit()

    # Get all pipeline IDs after
    pipeline_ids_after = set(db_utils.get_all_pipeline_ids(cur))
    new_pipeline_ids = pipeline_ids_after - pipeline_ids_before
    assert len(new_pipeline_ids) == 1, "A new pipeline should be created."
    new_pip_id = next(iter(new_pipeline_ids))

    # Load the new pipeline as a graph
    new_graph = db_to_pipeline_graph_from_pip_id(cur, new_pip_id)
    conn.commit()

    # Get descendants of the start_node in the original graph
    descendants = set(nx.descendants(original_graph, start_node))
    nodes_to_replace = descendants | {start_node}
    original_nodes = set(original_graph.nodes)
    new_nodes = set(new_graph.nodes)

    # The new graph should have the same number of nodes as the original
    assert len(new_nodes) == len(original_nodes), "New pipeline should have the same number of nodes as the original."

    # The original graph in the database should not have changed
    original_graph_check = db_to_pipeline_graph_from_pip_id(cur, original_graph.graph['pipeline_id'])
    assert nx.is_isomorphic(original_graph_check, original_graph), "Original graph should remain unchanged in the database."

    # Nodes not in nodes_to_replace should be preserved (same IDs)
    for node in original_nodes - nodes_to_replace:
        assert node in new_nodes, f"Node {node} should be preserved in the new pipeline."

    # Nodes in nodes_to_replace should be replaced with new IDs (not present in original)
    replaced_nodes = new_nodes - (original_nodes - nodes_to_replace)
    for node in replaced_nodes:
        assert node not in original_nodes, "Replaced node IDs should be new in the new pipeline."

    # The set of replaced nodes should be the same size as nodes_to_replace
    assert len(replaced_nodes) == len(nodes_to_replace), "Each replaced node should have a new node ID in the new pipeline."

    # The nodes_to_replace should have updated node_folder_path properties
    for new_node in new_nodes:
        new_folder = db_utils.get_node_folder_path(cur, new_node)
        assert new_folder.endswith(new_node), f"Node {new_node} folder path should be updated."

    # Test that the old pipeline is a parent of the new pipeline
    parent_pipelines = db_utils.get_pipeline_parents(cur, new_pip_id)
    assert original_graph.graph['pipeline_id'] in parent_pipelines, "Original pipeline should be a parent of the new pipeline."

    # Check new pipeline is referenced
    new_status = db_utils.get_pipeline_blocked_status(cur, new_pip_id)
    assert new_status == False, "New pipeline should be set to referenced= false after branching."



from conftest import PARENT_NODE_LIST
@pytest.mark.parametrize("start_node", PARENT_NODE_LIST)
def test_branch_pipeline_from_node_code_data(monkeypatch, pg_test_db, dag_dummy_1, start_node, tmp_base_dir):
    """
    Test that branch_pipeline_from_node creates a new pipeline where all nodes are preserved,
    except the provided node and all its descendants, which are replaced with new IDs.
    """
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    from fusionpipe.utils.pip_utils import branch_pipeline_from_node, db_to_pipeline_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx
    from fusionpipe.utils.pip_utils import pipeline_graph_to_db
    from fusionpipe.utils.pip_utils import init_node_folder
    

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the original graph to the database
    original_graph = dag_dummy_1

    pipeline_graph_to_db(original_graph, cur)
    conn.commit()

    # Get all pipeline IDs before
    pipeline_ids_before = set(db_utils.get_all_pipeline_ids(cur))

    # Run branch_pipeline_from_node
    branch_pipeline_from_node(cur, original_graph.graph['pipeline_id'], start_node)
    conn.commit()

    # Get all pipeline IDs after
    pipeline_ids_after = set(db_utils.get_all_pipeline_ids(cur))
    new_pipeline_ids = pipeline_ids_after - pipeline_ids_before
    assert len(new_pipeline_ids) == 1, "A new pipeline should be created."
    new_pip_id = next(iter(new_pipeline_ids))

    # Load the new pipeline as a graph
    new_graph = db_to_pipeline_graph_from_pip_id(cur, new_pip_id)
    conn.commit()

    # Get descendants of the start_node in the original graph
    descendants = set(nx.descendants(original_graph, start_node))
    nodes_to_replace = descendants | {start_node}
    original_nodes = set(original_graph.nodes)
    new_nodes = set(new_graph.nodes)
    kept_nodes = original_nodes - nodes_to_replace
    replaced_nodes = new_nodes - kept_nodes

    # Create folder paths for the kept nodes
    for node in kept_nodes:
        folder_path_nodes = os.path.join(tmp_base_dir, node)
        init_node_folder(folder_path_nodes, verbose=True)
        db_utils.update_folder_path_node(cur, node, folder_path_nodes)

    # Folder path for the new node must exist
    for new_node in replaced_nodes:
        new_folder = db_utils.get_node_folder_path(cur, new_node)
        assert os.path.exists(new_folder), f"Folder path for new node {new_node} must exist."

    # .venv must be present in folder of new node
    for new_node in replaced_nodes:
        new_venv = os.path.join(db_utils.get_node_folder_path(cur, new_node), "code/.venv")
        assert os.path.exists(new_venv), f".venv folder for new node {new_node} must exist."

    # Nodes that become refrenced should no have write permission
    for node in kept_nodes:
        folder_path_node = db_utils.get_node_folder_path(cur, node)
        permissions = os.stat(os.path.join(folder_path_node, 'code')).st_mode & 0o444
        from fusionpipe.utils.pip_utils import FILE_CHMOD_BLOCKED
        assert permissions == FILE_CHMOD_BLOCKED, f"Folder {folder_path_node} should not have R/W permissions for user and group."


def test_duplicate_node_in_pipeline_w_code_and_data(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test that duplicate_node_in_pipeline_w_code_and_data duplicates a node in the pipeline,
    including its code and data folder, depending on the withdata argument.
    """
    import os
    import shutil
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, duplicate_node_in_pipeline_w_code_and_data, init_node_folder
    from fusionpipe.utils import db_utils
    import toml

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    # Setup: create a pipeline and a node with a folder
    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id = generate_pip_id()
    node_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test", owner="tester", notes="test pipeline")
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", referenced=False, notes="test node", folder_path=None, node_tag="test")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, position_x=0, position_y=0)
    db_utils.update_folder_path_node(cur, node_id, os.path.join(tmp_base_dir, node_id))
    conn.commit()
  
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    init_node_folder(folder_path_nodes, verbose=True)

    # Create a file in the data folder to test duplication
    data_folder = os.path.join(folder_path_nodes, "data")
    os.makedirs(data_folder, exist_ok=True)
    data_file = os.path.join(data_folder, "testfile.txt")
    with open(data_file, "w") as f:
        f.write("testdata")

    # --- Test withdata=False (default) ---
    new_node_id = generate_node_id()
    duplicate_node_in_pipeline_w_code_and_data(cur, pipeline_id, pipeline_id, node_id, new_node_id)
    conn.commit()

    # Check new node exists in pipeline
    nodes_in_pipeline = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)
    assert new_node_id in nodes_in_pipeline

    # Check new folder exists and file is copied
    new_folder_path_nodes = os.path.join(tmp_base_dir, new_node_id)
    assert os.path.exists(new_folder_path_nodes)
    # Check if pyproject.toml file exists in the new node's code folder
    new_code_folder_path = os.path.join(new_folder_path_nodes, "code")
    pyproject_file_path = os.path.join(new_code_folder_path, "pyproject.toml")
    assert os.path.isfile(pyproject_file_path), "pyproject.toml file does not exist in the new node's code folder."

    with open(pyproject_file_path, "r") as f:
        pyproject_content = toml.load(f)
    assert new_node_id in pyproject_content["project"]["name"], "pyproject.toml does not contain 'dependencies' section."

    # Data folder should exist but not contain the test file (since withdata=False)
    new_data_folder = os.path.join(new_folder_path_nodes, "data")
    assert os.path.exists(new_data_folder)
    assert not os.path.exists(os.path.join(new_data_folder, "testfile.txt")), "Data file should not be copied when withdata=False"

    # --- Test withdata=True ---
    new_node_id2 = generate_node_id()
    duplicate_node_in_pipeline_w_code_and_data(cur, pipeline_id, pipeline_id, node_id, new_node_id2, withdata=True)
    conn.commit()

    new_folder_path_nodes2 = os.path.join(tmp_base_dir, new_node_id2)
    new_data_folder2 = os.path.join(new_folder_path_nodes2, "data")
    assert os.path.exists(new_data_folder2)
    assert os.path.exists(os.path.join(new_data_folder2, "testfile.txt")), "Data file should be copied when withdata=True"

def test_duplicate_node_in_different_pipeline_w_code_and_data(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test that duplicate_node_in_pipeline_w_code_and_data can duplicate a node from one pipeline into another,
    including its code and data folder.
    """
    import os
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, duplicate_node_in_pipeline_w_code_and_data, init_node_folder
    from fusionpipe.utils import db_utils
    import toml

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    # Setup: create two pipelines and a node with a folder in the first pipeline
    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id_src = generate_pip_id()
    pipeline_id_dst = generate_pip_id()
    node_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id_src, tag="src", owner="tester", notes="src pipeline")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id_dst, tag="dst", owner="tester", notes="dst pipeline")
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", referenced=False, notes="test node", folder_path=None, node_tag="test")
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id_src, position_x=0, position_y=0)
    db_utils.update_folder_path_node(cur, node_id, os.path.join(tmp_base_dir, node_id))
    conn.commit()

    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    init_node_folder(folder_path_nodes, verbose=True)

    # Run duplication into a different pipeline
    new_node_id = generate_node_id()
    duplicate_node_in_pipeline_w_code_and_data(cur, pipeline_id_src, pipeline_id_dst, node_id, new_node_id)
    conn.commit()

    # Check new node exists in destination pipeline
    nodes_in_dst = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id_dst)
    assert new_node_id in nodes_in_dst

    # Check new folder exists and file is copied
    new_folder_path_nodes = os.path.join(tmp_base_dir, new_node_id)
    assert os.path.exists(new_folder_path_nodes)
    new_code_folder_path = os.path.join(new_folder_path_nodes, "code")
    pyproject_file_path = os.path.join(new_code_folder_path, "pyproject.toml")
    assert os.path.isfile(pyproject_file_path), "pyproject.toml file does not exist in the new node's code folder."

    with open(pyproject_file_path, "r") as f:
        pyproject_content = toml.load(f)
    assert new_node_id in pyproject_content["project"]["name"], "pyproject.toml does not contain the new node id in 'name'."

@pytest.mark.parametrize("selected_nodes",
                          [
                              ("A",),
                              ("A","B",),
                              ("B",),
                              ("E",),
                              ("C","D"),
                              ("A","C","D"),
                              ("E","C","D"),
                              ("B","D"),
                              ]
                          )
def test_duplicate_nodes_in_pipeline_with_relations(monkeypatch, pg_test_db, dag_dummy_1, tmp_base_dir, selected_nodes):
    """
    Test that duplicate_subtree_in_pipeline duplicates a subtree rooted at a node,
    with new node IDs and correct parent-child relations, and that head nodes preserve parent relations from outside the subtree.
    """
    import os
    from fusionpipe.utils.pip_utils import (
        pipeline_graph_to_db, db_to_pipeline_graph_from_pip_id, duplicate_nodes_in_pipeline_with_relations, generate_node_id, init_node_folder
    )
    from fusionpipe.utils import db_utils
    import networkx as nx

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    pipeline_graph_to_db(dag_dummy_1, cur)
    conn.commit()

    pipeline_id = dag_dummy_1.graph['pipeline_id']

    # Setup folders for all nodes to be duplicated
    for node_id in selected_nodes:
        folder_path = os.path.join(tmp_base_dir, node_id)
        init_node_folder(folder_path)
        db_utils.update_folder_path_node(cur, node_id, folder_path)
    conn.commit()

    # Duplicate nodes with relation
    id_map = duplicate_nodes_in_pipeline_with_relations(cur, pipeline_id, pipeline_id, selected_nodes)
    conn.commit()

    # Check: all nodes in the subtree have new IDs, and are present in the pipeline
    for old_id, new_id in id_map.items():
        assert old_id != new_id
        # The new node should exist in the pipeline
        all_nodes = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)
        assert new_id in all_nodes
        # The new node's folder should exist
        new_folder = os.path.join(tmp_base_dir, new_id)
        assert os.path.exists(new_folder)

    # Check: parent-child relations are preserved in the duplicated subtree
    graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)
    for old_parent, old_child in dag_dummy_1.subgraph(selected_nodes).edges:
        assert (id_map[old_parent], id_map[old_child]) in graph.edges

    # Check: original nodes are still present and unchanged
    for node_id in selected_nodes:
        assert node_id in graph.nodes

    # Head nodes of subtree preserve parent relations from outside the subtree
    subtree = dag_dummy_1.subgraph(selected_nodes)
    head_nodes = [n for n in subtree.nodes if subtree.in_degree(n) == 0]
    for old_head in head_nodes:
        # Find parents of the head node in the original graph that are outside the selected_nodes
        external_parents = [p for p in dag_dummy_1.predecessors(old_head) if p not in selected_nodes]
        new_head = id_map[old_head]
        for ext_parent in external_parents:
            # The new duplicated head node should have the same parent (from outside the subtree)
            assert (ext_parent, new_head) in graph.edges, f"External parent {ext_parent} should be connected to duplicated head node {new_head}"

def test_delete_node_data_removes_data_contents(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test that delete_node_data removes all contents of the node's data folder but not the folder itself.
    """
    import os
    from fusionpipe.utils.pip_utils import generate_node_id, delete_node_data, init_node_folder
    from fusionpipe.utils import db_utils

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    # Setup: create a node with a data folder and some files
    conn = pg_test_db
    cur = db_utils.init_db(conn)
    node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", referenced=False, notes="test node", folder_path=None)
    db_utils.update_folder_path_node(cur, node_id, os.path.join(tmp_base_dir, node_id))
    conn.commit()

    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    init_node_folder(folder_path_nodes, verbose=True)
    data_folder = os.path.join(folder_path_nodes, "data")
    os.makedirs(data_folder, exist_ok=True)
    # Create dummy files and subfolders
    file1 = os.path.join(data_folder, "file1.txt")
    file2 = os.path.join(data_folder, "file2.txt")
    subfolder = os.path.join(data_folder, "subfolder")
    os.makedirs(subfolder, exist_ok=True)
    file3 = os.path.join(subfolder, "file3.txt")
    with open(file1, "w") as f:
        f.write("test1")
    with open(file2, "w") as f:
        f.write("test2")
    with open(file3, "w") as f:
        f.write("test3")

    # Ensure files exist before deletion
    assert os.path.exists(file1)
    assert os.path.exists(file2)
    assert os.path.exists(file3)

    # Run delete_node_data
    delete_node_data(cur, node_id)

    # Data folder should still exist, but be empty
    assert os.path.exists(data_folder)
    assert not any(os.scandir(data_folder)), "Data folder is not empty after deletion"

def test_delete_node_from_pipeline_with_referenced_logic(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test delete_node_from_pipeline_with_referenced_logic for referenced and non-referenced nodes.
    """
    import os
    from fusionpipe.utils.pip_utils import (
        generate_node_id, generate_pip_id, init_node_folder, delete_node_from_pipeline_with_referenced_logic
    )
    from fusionpipe.utils import db_utils

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test", owner="tester", notes="test pipeline")

    # Case 1: Not Referenced node
    node_id_not_referenced = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id_not_referenced, status="ready", referenced=False, notes="not referenced node", folder_path=None)
    db_utils.add_node_to_pipeline(cur, node_id=node_id_not_referenced, pipeline_id=pipeline_id, position_x=0, position_y=0)
    db_utils.update_folder_path_node(cur, node_id_not_referenced, os.path.join(tmp_base_dir, node_id_not_referenced))
    conn.commit()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id_not_referenced)
    init_node_folder(folder_path_nodes, verbose=True)
    assert os.path.exists(folder_path_nodes)
    # Should delete node and folder
    delete_node_from_pipeline_with_referenced_logic(cur, pipeline_id, node_id_not_referenced)
    conn.commit()
    assert not os.path.exists(folder_path_nodes)
    assert node_id_not_referenced not in db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)

    # Case 2: Node is referenced and a leaf in the referenced subgraph
    node_id_referenced = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id_referenced, status="ready", referenced=True, notes="referenced node", folder_path=None)
    db_utils.add_node_to_pipeline(cur, node_id=node_id_referenced, pipeline_id=pipeline_id, position_x=1, position_y=1)
    conn.commit()
    # Should delete node from pipeline (no error)
    delete_node_from_pipeline_with_referenced_logic(cur, pipeline_id, node_id_referenced)
    conn.commit()
    assert node_id_referenced not in db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)

    # Case 3: Referenced node that is not a leaf in referenced subgraph
    to_be_deleted_id = generate_node_id()
    child_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=to_be_deleted_id, status="ready", referenced=True, notes="parent", folder_path=None, node_tag="parent")
    db_utils.add_node_to_nodes(cur, node_id=child_id, status="ready", referenced=True, notes="child", folder_path=None, node_tag="child")
    db_utils.add_node_to_pipeline(cur, node_id=to_be_deleted_id, pipeline_id=pipeline_id, position_x=2, position_y=2)
    db_utils.add_node_to_pipeline(cur, node_id=child_id, pipeline_id=pipeline_id, position_x=3, position_y=3)
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=to_be_deleted_id, edge_id='01')
    conn.commit()
    # Should raise ValueError because to_be_deleted_id is not a leaf
    import pytest
    with pytest.raises(ValueError):
        delete_node_from_pipeline_with_referenced_logic(cur, pipeline_id, to_be_deleted_id)


def test_delete_referenced_node_from_pipeline(tmp_base_dir, pg_test_db):
    """
    Test deleting a referenced node from one pipeline and ensuring its status is updated in another pipeline.
    """
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, delete_node_from_pipeline_with_referenced_logic
    from fusionpipe.utils.pip_utils import init_node_folder

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create two pipelines
    pipeline_id1 = generate_pip_id()
    pipeline_id2 = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id1)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id2)

    # Create a node and add it to both pipelines
    node_id = generate_node_id()
    folder_path_node = os.path.join(tmp_base_dir, node_id)
    init_node_folder(folder_path_node, verbose=True)

    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", referenced=True, folder_path=folder_path_node)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id1)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id2)
    conn.commit()

    # Delete the node from pipeline 2
    delete_node_from_pipeline_with_referenced_logic(cur, pipeline_id2, node_id)
    conn.commit()

    # Check that the node is deleted from pipeline 2
    nodes_in_pipeline2 = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id2)
    assert node_id not in nodes_in_pipeline2, f"Node {node_id} should be deleted from pipeline {pipeline_id2}."

    # Check that the node's referenced status is updated to False in pipeline 1 as the only instance
    referenced_status = db_utils.get_node_referenced_status(cur, node_id=node_id)
    assert referenced_status is False, f"Node {node_id} should have referenced status set to False in pipeline {pipeline_id1}."

    # Ensure R/W permission is granted to user and group for the node
    permissions = os.stat(os.path.join(folder_path_node, 'code')).st_mode & 0o2770
    from fusionpipe.utils.pip_utils import DIR_CHMOD_DEFAULT
    assert permissions == DIR_CHMOD_DEFAULT, f"Folder {folder_path_node} should have R/W permissions for user and group."


def test_delete_node_from_pipeline_with_referenced_logic_blocked_node(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test delete_node_from_pipeline_with_referenced_logic for the case when the node is blocked.
    """
    import os
    import warnings
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import (
        generate_node_id, generate_pip_id, init_node_folder, delete_node_from_pipeline_with_referenced_logic
    )

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test", owner="tester", notes="test pipeline")

    # Create a blocked node
    node_id_blocked = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id_blocked, status="ready", referenced=False, notes="blocked node", folder_path=None)
    db_utils.add_node_to_pipeline(cur, node_id=node_id_blocked, pipeline_id=pipeline_id, position_x=0, position_y=0)
    db_utils.update_node_blocked_status(cur, node_id=node_id_blocked, blocked=True)
    db_utils.update_folder_path_node(cur, node_id_blocked, os.path.join(tmp_base_dir, node_id_blocked))
    conn.commit()

    folder_path_nodes = os.path.join(tmp_base_dir, node_id_blocked)
    init_node_folder(folder_path_nodes, verbose=True)
    assert os.path.exists(folder_path_nodes)
    # Attempt to delete the blocked node and expect an error
    with pytest.raises(ValueError, match=f"Node {node_id_blocked} is blocked and cannot be deleted."):
        delete_node_from_pipeline_with_referenced_logic(cur, pipeline_id, node_id_blocked)
        conn.commit()

    # Ensure the node was not deleted
    assert node_id_blocked in db_utils.get_all_nodes_from_pip_id(cur, pipeline_id), "Blocked node should not be deleted."
    assert os.path.exists(folder_path_nodes), "Blocked node's folder should not be deleted."


def test_set_children_stale_sets_descendants_to_staledata(pg_test_db, dag_dummy_1):
    """
    Test that set_children_stale sets all descendants of a node to 'staledata' status.
    """
    from fusionpipe.utils.pip_utils import pipeline_graph_to_db, set_children_stale
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    pipeline_graph_to_db(dag_dummy_1, cur)
    conn.commit()

    pipeline_id = dag_dummy_1.graph['pipeline_id']
    # Pick a node with descendants
    for node in dag_dummy_1.nodes:
        descendants = set(nx.descendants(dag_dummy_1, node))
        if descendants:
            break
    else:
        pytest.skip("No node with descendants in dummy graph.")

    # Set all children to staledata
    set_children_stale(cur, pipeline_id, node)
    conn.commit()

    # Check all descendants are staledata
    for child in descendants:
        status = db_utils.get_node_status(cur, child)
        assert status == "staledata", f"Node {child} should be staledata, got {status}"
    # The node itself should not be changed
    status_self = db_utils.get_node_status(cur, node)
    assert status_self != "staledata", f"Node {node} itself should not be set to staledata"

def test_update_stale_status_for_pipeline_nodes(pg_test_db, dag_dummy_1):
    """
    Test that update_stale_status_for_pipeline_nodes propagates 'staledata' from a parent to all descendants.
    """
    from fusionpipe.utils.pip_utils import pipeline_graph_to_db, update_stale_status_for_pipeline_nodes
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    pipeline_graph_to_db(dag_dummy_1, cur)
    conn.commit()

    pipeline_id = dag_dummy_1.graph['pipeline_id']
    # Pick a node with descendants
    for node in dag_dummy_1.nodes:
        descendants = set(nx.descendants(dag_dummy_1, node))
        if descendants:
            break
    else:
        pytest.skip("No node with descendants in dummy graph.")

    # Set the parent node to 'staledata'
    db_utils.update_node_status(cur, node, "staledata")
    conn.commit()

    # Run the propagation function
    update_stale_status_for_pipeline_nodes(cur, pipeline_id)
    conn.commit()

    # All descendants should now be 'staledata'
    for child in descendants:
        status = db_utils.get_node_status(cur, child)
        assert status == "staledata", f"Node {child} should be staledata, got {status}"
    # The node itself should remain 'staledata'
    status_self = db_utils.get_node_status(cur, node)
    assert status_self == "staledata", f"Node {node} itself should be staledata"

    # Unrelated nodes (not descendants and not the node itself) should not be changed
    unaffected = set(dag_dummy_1.nodes) - descendants - {node}
    for n in unaffected:
        status = db_utils.get_node_status(cur, n)
        assert status != "staledata", f"Unrelated node {n} should not be set to staledata"

def test_add_node_relation_safe(pg_test_db):
    """
    Test add_node_relation_safe:
    - Adds a valid relation.
    - Fails if child is referenced.
    - Fails if adding the edge would create a cycle.
    """
    from fusionpipe.utils.pip_utils import add_node_relation_safe, generate_node_id, generate_pip_id
    from fusionpipe.utils.pip_utils import pipeline_graph_to_db, db_to_pipeline_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx
    import pytest

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a simple pipeline with two nodes
    pipeline_id = generate_pip_id()
    parent_id = generate_node_id()
    child_id = generate_node_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id)
    db_utils.add_node_to_nodes(cur, node_id=parent_id, status="ready", referenced=False)
    db_utils.add_node_to_nodes(cur, node_id=child_id, status="ready", referenced=False)
    db_utils.add_node_to_pipeline(cur, node_id=parent_id, pipeline_id=pipeline_id)
    db_utils.add_node_to_pipeline(cur, node_id=child_id, pipeline_id=pipeline_id)
    conn.commit()

    # Should succeed: add parent -> child
    assert add_node_relation_safe(cur, pipeline_id, parent_id, child_id) is True
    conn.commit()
    G = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)
    assert (parent_id, child_id) in G.edges

    # Should fail: adding child -> parent (would create a cycle)
    with pytest.raises(ValueError, match="cycle"):
        add_node_relation_safe(cur, pipeline_id, child_id, parent_id)

    # Should fail: child referenced
    db_utils.update_referenced_status(cur, node_id=child_id, referenced=True)
    with pytest.raises(ValueError, match="referenced"):
        add_node_relation_safe(cur, pipeline_id, parent_id, child_id)

def test_merge_pipelines(pg_test_db):
    """
    Test that merge_pipelines creates a new pipeline containing all nodes from the source pipelines.
    """
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, merge_pipelines
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create two source pipelines with different nodes
    pipeline_id1 = generate_pip_id()
    pipeline_id2 = generate_pip_id()
    node_ids1 = [generate_node_id() for _ in range(2)]
    node_ids2 = [generate_node_id() for _ in range(2)]
    db_utils.add_project(cur, project_id="p1")

    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id1, project_id="p1")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id2, project_id="p1")
    for node_id in node_ids1:
        db_utils.add_node_to_nodes(cur, node_id=node_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id1)
    for node_id in node_ids2:
        db_utils.add_node_to_nodes(cur, node_id=node_id)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id2)
    conn.commit()

    # Merge the pipelines
    merged_pipeline_id = merge_pipelines(cur, [pipeline_id1, pipeline_id2])
    conn.commit()

    # Get all nodes in the merged pipeline
    merged_nodes = set(db_utils.get_all_nodes_from_pip_id(cur, merged_pipeline_id))

    # All original nodes should be present in the merged pipeline
    for node_id in node_ids1 + node_ids2:
        assert node_id in merged_nodes

    # The merged pipeline should be a new pipeline
    assert merged_pipeline_id not in [pipeline_id1, pipeline_id2]

    # Check that the merged pipeline is a child of the original pipelines
    parent_pipelines = db_utils.get_pipeline_parents(cur, merged_pipeline_id)
    assert pipeline_id1 in parent_pipelines, "Merged pipeline should have pipeline_id1 as a parent."
    assert pipeline_id2 in parent_pipelines, "Merged pipeline should have pipeline_id2 as a parent."


def test_merge_pipelines_with_duplicate_nodes(pg_test_db):
    """
    Test that merge_pipelines handles duplicate nodes correctly:
    - If a node exists in multiple source pipelines, it is only added once to the merged pipeline.
    - The merge operation does not fail due to duplicate node IDs.
    """
    from fusionpipe.utils.pip_utils import generate_node_id, generate_pip_id, merge_pipelines
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create two source pipelines with overlapping nodes
    pipeline_id1 = generate_pip_id()
    pipeline_id2 = generate_pip_id()
    common_node_id = generate_node_id()
    unique_node_id1 = generate_node_id()
    unique_node_id2 = generate_node_id()
    db_utils.add_project(cur, project_id="p1")

    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id1, project_id="p1")
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id2, project_id="p1")

    # Add nodes to the first pipeline
    db_utils.add_node_to_nodes(cur, node_id=common_node_id)
    db_utils.add_node_to_nodes(cur, node_id=unique_node_id1)
    db_utils.add_node_to_pipeline(cur, node_id=common_node_id, pipeline_id=pipeline_id1)
    db_utils.add_node_to_pipeline(cur, node_id=unique_node_id1, pipeline_id=pipeline_id1)

    # Add nodes to the second pipeline
    db_utils.add_node_to_nodes(cur, node_id=unique_node_id2)
    db_utils.add_node_to_pipeline(cur, node_id=common_node_id, pipeline_id=pipeline_id2)
    db_utils.add_node_to_pipeline(cur, node_id=unique_node_id2, pipeline_id=pipeline_id2)

    conn.commit()

    # Merge the pipelines
    merged_pipeline_id = merge_pipelines(cur, [pipeline_id1, pipeline_id2])
    conn.commit()

    # Get all nodes in the merged pipeline
    merged_nodes = set(db_utils.get_all_nodes_from_pip_id(cur, merged_pipeline_id))

    # Check that all unique nodes are present in the merged pipeline
    expected_nodes = {common_node_id, unique_node_id1, unique_node_id2}
    assert merged_nodes == expected_nodes, f"Merged pipeline nodes do not match expected nodes: {expected_nodes}"

    # Check that the merged pipeline does not contain duplicate nodes
    assert len(merged_nodes) == len(expected_nodes), "Merged pipeline contains duplicate nodes"

    # The merged pipeline should be a new pipeline
    assert merged_pipeline_id not in [pipeline_id1, pipeline_id2]




from conftest import dag_detach_1, dag_detach_2
@pytest.mark.parametrize("dag, start_node, expected_detached_nodes, not_detached_nodes", [
    (dag_detach_1,"A",["A","B","C"],["D"]),
    (dag_detach_1,"B",["B","C"],["D"]),
    (dag_detach_2,"A",["A","B","C"],["D"]),      
    ])
def test_detach_pipeline_from_node(
    monkeypatch,
    pg_test_db,
    tmp_base_dir,
    dag,
    start_node,
    expected_detached_nodes,
    not_detached_nodes
    ):
    from fusionpipe.utils.pip_utils import detach_subgraph_from_node, db_to_pipeline_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx
    from fusionpipe.utils.pip_utils import pipeline_graph_to_db
    from fusionpipe.utils.pip_utils import init_node_folder


    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the original graph to the database
    original_graph = dag()

    pipeline_graph_to_db(original_graph, cur)
    conn.commit()

    # Setup folders for all nodes to be duplicated
    for node_id in original_graph:
        folder_path = os.path.join(tmp_base_dir, node_id)
        init_node_folder(folder_path)
        db_utils.update_folder_path_node(cur, node_id, folder_path)
    conn.commit()

    id_map = detach_subgraph_from_node(cur, original_graph.graph['pipeline_id'], start_node)

    # Check that the keys in id_map correspond to the expected detached nodes
    assert set(id_map.keys()) == set(expected_detached_nodes), f"Detached nodes do not match expected nodes. Expected: {expected_detached_nodes}, Found: {list(id_map.keys())}"

    # Get the parents of the nodes that are not deteached in the original graph
    original_parents_dict_not_detached_nodes = { node: list(original_graph.predecessors(node)) for node in not_detached_nodes}
    
    # Get the modified graph from the database
    modified_graph = db_to_pipeline_graph_from_pip_id(cur, original_graph.graph['pipeline_id'])

    # Get the parents of the nodes that are not detached in the modified graph
    new_parents_dict_not_detached_nodes = { node: list(modified_graph.predecessors(node)) for node in not_detached_nodes}

    # The expected new parentes of the nodes which are not detached should be the original ones, where the nodes that have been detached
    # are replaced with the new detached ones
    for node in not_detached_nodes:
        expected_parents = original_parents_dict_not_detached_nodes[node].copy()
        for i, parent in enumerate(expected_parents):
            if parent in id_map:
                expected_parents[i] = id_map[parent]
        assert set(new_parents_dict_not_detached_nodes[node]) == set(expected_parents), f"Parents of node {node} do not match expected parents. Expected: {expected_parents}, Found: {new_parents_dict_not_detached_nodes[node]}"

    # # Check that the folder paths for the detached nodes have been set correctly
    for old_id, new_id in id_map.items():
        folder_path_old = os.path.join(tmp_base_dir, old_id)
        folder_path_new = os.path.join(tmp_base_dir, new_id)
        assert os.path.exists(folder_path_new), f"Folder for new node {new_id} does not exist."
        assert os.path.exists(folder_path_old), f"Folder for old node {old_id} does not exist."

    # Check that the folder_path entry in the database for the new nodes is different from the old nodes
    for old_id, new_id in id_map.items():
        old_folder_path = db_utils.get_node_folder_path(cur, old_id)
        new_folder_path = db_utils.get_node_folder_path(cur, new_id)
        assert old_folder_path != new_folder_path, f"folder_path for new node {new_id} should be different from old node {old_id}"

def test_reference_node_into_pipeline(tmp_base_dir, pg_test_db):
    """
    Test reference_nodes_into_pipeline:
    - Successfully references a node from one pipeline into another.
    - Fails if the node does not exist in the source pipeline.
    - Fails if the source and target pipelines are the same.
    """
    from fusionpipe.utils.pip_utils import reference_nodes_into_pipeline, generate_node_id, generate_pip_id
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import init_node_folder    

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create two pipelines
    source_pipeline_id = generate_pip_id()
    target_pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=source_pipeline_id)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=target_pipeline_id)

    # Create a node in the source pipeline
    node_id = generate_node_id()
    folder_path_node = os.path.join(tmp_base_dir, node_id)
    init_node_folder(folder_path_node, verbose=True)    
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", referenced=False, folder_path=folder_path_node)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=source_pipeline_id)
    conn.commit()

    # Case 1: Successfully reference the node into the target pipeline
    reference_nodes_into_pipeline(cur, source_pipeline_id, target_pipeline_id, [node_id])
    conn.commit()

    # Check that the node exists in the target pipeline
    nodes_in_target = db_utils.get_all_nodes_from_pip_id(cur, target_pipeline_id)
    assert node_id in nodes_in_target, f"Node {node_id} should exist in the target pipeline {target_pipeline_id}."

    # Check that the node's is not blocked after referencing
    blocked_status = db_utils.get_node_blocked_status(cur, node_id=node_id)
    assert blocked_status is False, f"Node {node_id} should have blocked status set to False."

    # Ensure referenced node has not Writing permission for user and group
    permissions = os.stat(os.path.join(folder_path_node, 'code')).st_mode & 0o777

    from fusionpipe.utils.pip_utils import DIR_CHMOD_BLOCKED
    assert permissions == DIR_CHMOD_BLOCKED, f"Folder {folder_path_node} should have R/W permissions for user and group."

    # Case 2: Fail if the node does not exist in the source pipeline
    non_existent_node_id = generate_node_id()
    with pytest.raises(ValueError, match=f"Node {non_existent_node_id} does not exist in the source pipeline {source_pipeline_id}."):
        reference_nodes_into_pipeline(cur, source_pipeline_id, target_pipeline_id, [non_existent_node_id])

    # Case 3: Fail if the source and target pipelines are the same
    with pytest.raises(ValueError, match="Source pipeline and target pipeline cannot be the same."):
        reference_nodes_into_pipeline(cur, source_pipeline_id, source_pipeline_id, [node_id])

    # Case 4: Fail if the node is not head of the subgraph
    # Create a second node in the source pipeline and create a relation
    second_node_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=second_node_id, status="ready", referenced=False)
    db_utils.add_node_to_pipeline(cur, node_id=second_node_id, pipeline_id=source_pipeline_id)
    db_utils.add_node_relation(cur, parent_id=node_id, child_id=second_node_id, edge_id='01')
    conn.commit()
    # Attempt to reference the second, which is not a head of the subgraph
    with pytest.raises(ValueError, match=f"Node {second_node_id} cannot be referenced from pipeline {source_pipeline_id}. It is not a head of a subgraph. Consider duplicating the node with data first."):
        reference_nodes_into_pipeline(cur, source_pipeline_id, target_pipeline_id, [second_node_id])

def test_reference_and_delete_node_logic(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test the following sequence:
    - Create a node.
    - Initialize the node folder.
    - Create a pipeline.
    - Create a second pipeline.
    - Reference the node in the second pipeline.
    - Check that the status of the node is referenced=True.
    - Check that the code folder in the referenced node is DIR_CHMOD_BLOCKED.
    - Delete the node from the second pipeline with delete_node_from_pipeline_with_referenced_logic.
    - Check that the code folder in the node is now in DIR_CHMOD_DEFAULT.
    """
    from fusionpipe.utils.pip_utils import (
        generate_node_id,
        generate_pip_id,
        init_node_folder,
        reference_nodes_into_pipeline,
        delete_node_from_pipeline_with_referenced_logic,
        DIR_CHMOD_BLOCKED,
        DIR_CHMOD_DEFAULT,
    )
    from fusionpipe.utils import db_utils

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Step 1: Create a node and initialize its folder
    node_id = generate_node_id()
    folder_path_node = os.path.join(tmp_base_dir, node_id)
    init_node_folder(folder_path_node, verbose=True)
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", referenced=False, folder_path=folder_path_node)
    conn.commit()

    # Step 2: Create two pipelines
    pipeline_id1 = generate_pip_id()
    pipeline_id2 = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id1)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id2)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id1)
    conn.commit()

    # Step 3: Reference the node in the second pipeline
    reference_nodes_into_pipeline(cur, pipeline_id1, pipeline_id2, [node_id])
    conn.commit()

    # Step 4: Check that the node's status is referenced=True
    referenced_status = db_utils.get_node_referenced_status(cur, node_id=node_id)
    assert referenced_status is True, f"Node {node_id} should have referenced status set to True."

    # Step 5: Check that the code folder in the referenced node is DIR_CHMOD_BLOCKED
    permissions = os.stat(os.path.join(folder_path_node, "code")).st_mode & 0o555
    assert permissions == DIR_CHMOD_BLOCKED, f"Code folder should have permissions {oct(DIR_CHMOD_BLOCKED)}, got {oct(permissions)}."

    # Step 6: Delete the node from the second pipeline
    delete_node_from_pipeline_with_referenced_logic(cur, pipeline_id2, node_id)
    conn.commit()

    # Step 7: Check that the code folder in the node is now DIR_CHMOD_DEFAULT
    permissions = os.stat(os.path.join(folder_path_node, "code")).st_mode & 0o2770
    assert permissions == DIR_CHMOD_DEFAULT, f"Code folder should have permissions {oct(DIR_CHMOD_DEFAULT)}, got {oct(permissions)}."



def test_detach_node_from_pipeline_logic(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test the detach_node_from_pipeline function:
    - Successfully detaches a referenced node.
    - Fails if the node is not referenced.
    - Fails if the node is blocked.
    """
    from fusionpipe.utils.pip_utils import (
        generate_node_id,
        generate_pip_id,
        init_node_folder,
        detach_node_from_pipeline,
        DIR_CHMOD_BLOCKED,
        DIR_CHMOD_DEFAULT,
    )
    from fusionpipe.utils import db_utils
    import os

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Step 1: Create a pipeline and a node
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id)

    node_id = generate_node_id()
    folder_path_node = os.path.join(tmp_base_dir, node_id)
    init_node_folder(folder_path_node, verbose=True)
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", referenced=True, folder_path=folder_path_node)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id)
    conn.commit()

    # Step 2: Detach the node
    new_node_id = detach_node_from_pipeline(cur, pipeline_id, node_id)
    conn.commit()

    # Check that the original node is removed from the pipeline
    nodes_in_pipeline = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)
    assert node_id not in nodes_in_pipeline, f"Node {node_id} should be removed from the pipeline."

    # Check that the new node is added to the pipeline
    assert new_node_id in nodes_in_pipeline, f"New node {new_node_id} should be added to the pipeline."

    # Check that the new node's folder exists
    new_folder_path_node = os.path.join(tmp_base_dir, new_node_id)
    assert os.path.exists(new_folder_path_node), f"New node folder {new_folder_path_node} should exist."

    # Check that the new node's permissions are DIR_CHMOD_DEFAULT
    permissions = os.stat(os.path.join(new_folder_path_node, "code")).st_mode & 0o2770
    assert permissions == DIR_CHMOD_DEFAULT, f"New node's code folder should have permissions {oct(DIR_CHMOD_DEFAULT)}, got {oct(permissions)}."

    # Step 3: Attempt to detach a non-referenced node
    non_referenced_node_id = generate_node_id()
    folder_path_non_referenced = os.path.join(tmp_base_dir, non_referenced_node_id)
    init_node_folder(folder_path_non_referenced, verbose=True)
    db_utils.add_node_to_nodes(cur, node_id=non_referenced_node_id, status="ready", referenced=False, folder_path=folder_path_non_referenced)
    db_utils.add_node_to_pipeline(cur, node_id=non_referenced_node_id, pipeline_id=pipeline_id)
    conn.commit()

    with pytest.raises(ValueError, match=f"Node {non_referenced_node_id} is not referenced. You can detach only nodes which are referenced."):
        detach_node_from_pipeline(cur, pipeline_id, non_referenced_node_id)

    # Step 4: Attempt to detach a blocked node
    blocked_node_id = generate_node_id()
    folder_path_blocked = os.path.join(tmp_base_dir, blocked_node_id)
    init_node_folder(folder_path_blocked, verbose=True)
    db_utils.add_node_to_nodes(cur, node_id=blocked_node_id, status="ready", referenced=True, folder_path=folder_path_blocked)
    db_utils.add_node_to_pipeline(cur, node_id=blocked_node_id, pipeline_id=pipeline_id)
    db_utils.update_node_blocked_status(cur, node_id=blocked_node_id, blocked=True)
    conn.commit()

    with pytest.raises(ValueError, match=f"Node {blocked_node_id} is blocked and cannot be detached."):
        detach_node_from_pipeline(cur, pipeline_id, blocked_node_id)




def test_reference_and_detach_node_logic(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test the following sequence:
    - Create a node.
    - Initialize the node folder.
    - Create a pipeline.
    - Create a second pipeline.
    - Reference the node in the second pipeline.
    - Check that the status of the node is referenced=True.
    - Check that the code folder in the referenced node is DIR_CHMOD_BLOCKED.
    - Detach the node from the second pipeline with detach_node_from_pipeline.
    - Check that the code folder in the node is now in DIR_CHMOD_DEFAULT.
    """
    from fusionpipe.utils.pip_utils import (
        generate_node_id,
        generate_pip_id,
        init_node_folder,
        reference_nodes_into_pipeline,
        detach_node_from_pipeline,
        DIR_CHMOD_BLOCKED,
        DIR_CHMOD_DEFAULT,
    )
    from fusionpipe.utils import db_utils

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Step 1: Create a node and initialize its folder
    node_id = generate_node_id()
    folder_path_node = os.path.join(tmp_base_dir, node_id)
    init_node_folder(folder_path_node, verbose=True)
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", referenced=False, folder_path=folder_path_node)
    conn.commit()

    # Step 2: Create two pipelines
    pipeline_id1 = generate_pip_id()
    pipeline_id2 = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id1)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id2)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id1)
    conn.commit()

    # Step 3: Reference the node in the second pipeline
    reference_nodes_into_pipeline(cur, pipeline_id1, pipeline_id2, [node_id])
    conn.commit()

    # Step 4: Check that the node's status is referenced=True
    referenced_status = db_utils.get_node_referenced_status(cur, node_id=node_id)
    assert referenced_status is True, f"Node {node_id} should have referenced status set to True."

    # Step 5: Check that the code folder in the referenced node is DIR_CHMOD_BLOCKED
    permissions = os.stat(os.path.join(folder_path_node, "code")).st_mode & 0o555
    assert permissions == DIR_CHMOD_BLOCKED, f"Code folder should have permissions {oct(DIR_CHMOD_BLOCKED)}, got {oct(permissions)}."

    # Step 6: Detach the node from the second pipeline
    new_node_id = detach_node_from_pipeline(cur, pipeline_id2, node_id)
    conn.commit()

    # Step 7: Check that the code folder in the node is now DIR_CHMOD_DEFAULT
    permissions = os.stat(os.path.join(folder_path_node, "code")).st_mode & 0o2770
    assert permissions == DIR_CHMOD_DEFAULT, f"Code folder should have permissions {oct(DIR_CHMOD_DEFAULT)}, got {oct(permissions)}."

    # Step 8: Check that the node is still in the first pipeline
    nodes_in_pipeline1 = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id1)
    assert node_id in nodes_in_pipeline1, f"Node {node_id} should still be in the first pipeline {pipeline_id1}."


def test_node_is_head_of_subgraph(pg_test_db, dag_dummy_1):
    """
    Test that node_is_head_of_subgraph correctly identifies head nodes in the subgraph of referenced nodes.
    """
    from fusionpipe.utils.pip_utils import pipeline_graph_to_db, node_is_head_of_subgraph
    from fusionpipe.utils import db_utils

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    pipeline_graph_to_db(dag_dummy_1, cur)
    conn.commit()

    pipeline_id = dag_dummy_1.graph['pipeline_id']

    # Mark some nodes as referenced
    referenced_nodes = [n for n in dag_dummy_1.nodes if dag_dummy_1.in_degree(n) == 0]
    for node in referenced_nodes:
        db_utils.update_referenced_status(cur, node_id=node, referenced=True)
    conn.commit()

    # Test each node
    for node in dag_dummy_1.nodes:
        is_head = node_is_head_of_subgraph(cur, pipeline_id, node)
        expected_is_head = node in referenced_nodes
        assert is_head == expected_is_head, f"Node {node} head status mismatch. Expected: {expected_is_head}, Found: {is_head}"


def test_duplicate_pipeline(pg_test_db, dag_dummy_1, tmp_base_dir, monkeypatch):
    """
    Test that duplicate_pipeline correctly duplicates a pipeline, including its nodes and relations.
    """
    from fusionpipe.utils.pip_utils import (
        duplicate_pipeline,
        pipeline_graph_to_db,
        db_to_pipeline_graph_from_pip_id,
        init_node_folder,
    )
    from fusionpipe.utils import db_utils
    import networkx as nx
    

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    pipeline_graph_to_db(dag_dummy_1, cur)
    conn.commit()

    pipeline_id = dag_dummy_1.graph['pipeline_id']

    # Initialize folders for all nodes in the pipeline
    for node_id in dag_dummy_1.nodes:
        folder_path = os.path.join(tmp_base_dir, node_id)
        init_node_folder(folder_path)
        db_utils.update_folder_path_node(cur, node_id, folder_path)
    conn.commit()

    # Duplicate the pipeline
    new_pipeline_id = duplicate_pipeline(cur, pipeline_id, withdata=True)
    conn.commit()

    # Load the original and duplicated pipelines
    original_graph = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)
    duplicated_graph = db_to_pipeline_graph_from_pip_id(cur, new_pipeline_id)

    # Check that the duplicated pipeline is isomorphic to the original
    def node_match(n1, n2):
        for attr in ['status', 'referenced', 'notes']:
            if n1.get(attr) != n2.get(attr):
                return False
        return True

    def edge_match(e1, e2):
        return True

    assert nx.is_isomorphic(
        original_graph, duplicated_graph,
        node_match=node_match,
        edge_match=edge_match
    ), "Duplicated pipeline is not isomorphic to the original pipeline."

    # Check that the new pipeline has a different ID
    assert new_pipeline_id != pipeline_id, "Duplicated pipeline should have a different ID from the original."

    # Check that the node folders were duplicated
    for node_id in dag_dummy_1.nodes:
        original_folder = os.path.join(tmp_base_dir, node_id)
        new_node_id = [n for n in duplicated_graph.nodes if duplicated_graph.nodes[n].get('tag') == dag_dummy_1.nodes[node_id].get('tag')][0]
        duplicated_folder = os.path.join(tmp_base_dir, new_node_id)
        assert os.path.exists(duplicated_folder), f"Duplicated folder for node {new_node_id} does not exist."
        assert os.path.exists(os.path.join(duplicated_folder, "data")), f"Data folder for node {new_node_id} was not duplicated."




def test_branch_pipeline(pg_test_db, dag_dummy_1, tmp_base_dir, monkeypatch):
    """
    Test that branch_pipeline correctly duplicates a pipeline and maintains parent-child relationships.
    """
    from fusionpipe.utils.pip_utils import (
        branch_pipeline,
        pipeline_graph_to_db,
        db_to_pipeline_graph_from_pip_id,
        init_node_folder,
    )
    from fusionpipe.utils import pip_utils, db_utils
    import networkx as nx

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    pipeline_graph_to_db(dag_dummy_1, cur)
    conn.commit()

    original_pipeline_id = dag_dummy_1.graph['pipeline_id']

    # Initialize folders for all nodes in the pipeline
    for node_id in dag_dummy_1.nodes:
        folder_path = os.path.join(tmp_base_dir, node_id)
        init_node_folder(folder_path)
        db_utils.update_folder_path_node(cur, node_id, folder_path)
    conn.commit()

    # Add a parent pipeline to the original pipeline
    parent_pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_pipeline_id)
    db_utils.add_pipeline_relation(cur, child_id=original_pipeline_id, parent_id=parent_pipeline_id)
    conn.commit()

    # Branch the pipeline
    new_pipeline_id = branch_pipeline(cur, original_pipeline_id, withdata=True)
    conn.commit()

    # Load the original and branched pipelines
    original_graph = db_to_pipeline_graph_from_pip_id(cur, original_pipeline_id)
    branched_graph = db_to_pipeline_graph_from_pip_id(cur, new_pipeline_id)

    # Check that the branched pipeline is isomorphic to the original
    def node_match(n1, n2):
        for attr in ['status', 'referenced', 'notes']:
            if n1.get(attr) != n2.get(attr):
                return False
        return True

    def edge_match(e1, e2):
        return True

    assert nx.is_isomorphic(
        original_graph, branched_graph,
        node_match=node_match,
        edge_match=edge_match
    ), "Branched pipeline is not isomorphic to the original pipeline."

    # Check that the new pipeline has a different ID
    assert new_pipeline_id != original_pipeline_id, "Branched pipeline should have a different ID from the original."

    # Check that the parent pipeline is linked to the new pipeline
    parents_of_new_pipeline = db_utils.get_pipeline_parents(cur, new_pipeline_id)
    assert original_pipeline_id in parents_of_new_pipeline, "Pipeline should be children of the pipeline from which it was branched."

    # Check that the node folders were duplicated
    for node_id in dag_dummy_1.nodes:
        original_folder = os.path.join(tmp_base_dir, node_id)
        new_node_id = [n for n in branched_graph.nodes if branched_graph.nodes[n].get('tag') == dag_dummy_1.nodes[node_id].get('tag')][0]
        branched_folder = os.path.join(tmp_base_dir, new_node_id)
        assert os.path.exists(branched_folder), f"Branched folder for node {new_node_id} does not exist."
        assert os.path.exists(os.path.join(branched_folder, "data")), f"Data folder for node {new_node_id} was not duplicated."


def test_create_node_in_pipeline(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test that create_node_in_pipeline creates a new node, adds it to the database and pipeline,
    initializes its folder, and returns the correct node_id.
    """
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import (
        create_node_in_pipeline,
        init_node_folder,
    )

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a pipeline
    pipeline_id = "p_testpipeline"
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id)
    conn.commit()

    # Call create_node_in_pipeline
    node_id = create_node_in_pipeline(cur, pipeline_id)
    conn.commit()

    # Check that the node exists in the nodes table
    all_nodes = db_utils.get_all_node_ids(cur)
    assert node_id in all_nodes, f"Node {node_id} should exist in the nodes table."

    # Check that the node is in the pipeline
    nodes_in_pipeline = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)
    assert node_id in nodes_in_pipeline, f"Node {node_id} should be in pipeline {pipeline_id}."

    # Check that the node's folder exists
    folder_path = os.path.join(tmp_base_dir, node_id)
    assert os.path.exists(folder_path), f"Node folder {folder_path} should exist."

    # Check that the code, data, and reports subfolders exist
    for subfolder in ["code", "data", "reports"]:
        subfolder_path = os.path.join(folder_path, subfolder)
        assert os.path.isdir(subfolder_path), f"Subfolder {subfolder_path} should exist."

    # Check that the node's folder_path in the database matches
    db_folder_path = db_utils.get_node_folder_path(cur, node_id)
    assert db_folder_path == folder_path, "Node folder_path in DB does not match expected path."

    # Check that the node's status and referenced fields are correct
    status = db_utils.get_node_status(cur, node_id)
    referenced = db_utils.get_node_referenced_status(cur, node_id)
    assert status == "ready", "Node status should be 'ready'."
    assert referenced is False, "Node referenced should be False."

def test_move_pipeline_to_project(pg_test_db):
    """
    Test that move_pipeline_to_project moves a pipeline to a new project,
    updates the project_id, and removes all pipeline relations.
    """
    from fusionpipe.utils import db_utils
    from fusionpipe.utils.pip_utils import generate_pip_id, move_pipeline_to_project


    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create two projects and a pipeline
    project_id1 = "pr_project1"
    project_id2 = "pr_project2"
    pipeline_id = generate_pip_id()
    db_utils.add_project(cur, project_id=project_id1)
    db_utils.add_project(cur, project_id=project_id2)
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, project_id=project_id1)
    # Add a parent pipeline relation
    parent_pipeline_id = generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=parent_pipeline_id, project_id=project_id1)
    db_utils.add_pipeline_relation(cur, child_id=pipeline_id, parent_id=parent_pipeline_id)
    conn.commit()

    # Move pipeline to new project
    move_pipeline_to_project(cur, pipeline_id, project_id2)
    conn.commit()

    # Check that the pipeline's project_id is updated
    new_project_id = db_utils.get_project_id_by_pipeline(cur, pipeline_id=pipeline_id)
    assert new_project_id == project_id2, "Pipeline project_id was not updated."

    # Check that all pipeline relations are removed
    parents = db_utils.get_pipeline_parents(cur, pipeline_id)
    assert not parents, "Pipeline parent relations were not removed."

    # Check error if pipeline does not exist
    with pytest.raises(ValueError, match="does not exist"):
        move_pipeline_to_project(cur, "nonexistent_pipeline", project_id2)

    # Check error if pipeline is blocked
    db_utils.update_pipeline_blocked_status(cur, pipeline_id=pipeline_id, blocked=True)
    with pytest.raises(ValueError, match="is blocked"):
        move_pipeline_to_project(cur, pipeline_id, project_id1)

def test_delete_edge_and_update_status(pg_test_db, dag_dummy_1):
    """
    Test delete_edge_and_update_status:
    - Removes the edge and sets descendants to 'staledata'.
    - Fails if pipeline is blocked.
    - Fails if child node is referenced.
    """
    from fusionpipe.utils.pip_utils import (
        pipeline_graph_to_db,
        delete_edge_and_update_status,
        db_to_pipeline_graph_from_pip_id,
        set_children_stale,
    )
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    pipeline_graph_to_db(dag_dummy_1, cur)
    conn.commit()
    pipeline_id = dag_dummy_1.graph['pipeline_id']

    # Pick a parent-child edge
    edges = list(dag_dummy_1.edges)
    if not edges:
        pytest.skip("No edges in dummy graph.")
    parent_id, child_id = edges[0]

    # --- Test: normal case ---
    # Ensure child is not referenced and pipeline is not blocked
    db_utils.update_referenced_status(cur, node_id=child_id, referenced=False)
    db_utils.update_pipeline_blocked_status(cur, pipeline_id=pipeline_id, blocked=False)
    conn.commit()

    # Set a descendant of child to 'ready' so we can check staledata propagation
    descendants = list(nx.descendants(dag_dummy_1, child_id))
    if descendants:
        db_utils.update_node_status(cur, descendants[0], "ready")
        conn.commit()

    # Remove the edge
    delete_edge_and_update_status(cur, pipeline_id, parent_id, child_id)
    conn.commit()

    # Edge should be removed
    G = db_to_pipeline_graph_from_pip_id(cur, pipeline_id)
    assert (parent_id, child_id) not in G.edges, "Edge should be removed from the graph."

    # All descendants of child should be 'staledata'
    for desc in descendants:
        status = db_utils.get_node_status(cur, desc)
        assert status == "staledata", f"Descendant node {desc} should be set to 'staledata'."

    # --- Test: pipeline is blocked ---
    # Re-add the edge for this test
    db_utils.add_node_relation(cur, parent_id=parent_id, child_id=child_id, edge_id="e1")
    db_utils.update_pipeline_blocked_status(cur, pipeline_id=pipeline_id, blocked=True)
    conn.commit()
    with pytest.raises(ValueError, match="blocked"):
        delete_edge_and_update_status(cur, pipeline_id, parent_id, child_id)
    db_utils.update_pipeline_blocked_status(cur, pipeline_id=pipeline_id, blocked=False)
    conn.commit()

    # --- Test: child node is referenced ---
    db_utils.update_referenced_status(cur, node_id=child_id, referenced=True)
    conn.commit()
    with pytest.raises(ValueError, match="referenced"):
        delete_edge_and_update_status(cur, pipeline_id, parent_id, child_id)
