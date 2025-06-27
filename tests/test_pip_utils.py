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




def test_graph_to_dict(dag_dummy_1, dict_dummy_1):
    from fusionpipe.utils.pip_utils import graph_to_dict
    import networkx as nx

    # Convert the graph to a dictionary
    graph_dict = graph_to_dict(dag_dummy_1)

    # Check if the converted dictionary matches the expected dictionary
    assert graph_dict == dict_dummy_1, "Graph to dict conversion did not produce the expected result."

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


def test_graph_to_db_and_db_to_graph_roundtrip(pg_test_db, dag_dummy_1):
    """
    Test that a graph can be written to the database and then read back,
    and that the structure and attributes are preserved.
    """
    from fusionpipe.utils.pip_utils import graph_to_db, db_to_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx

    # Setup database
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Write the dummy graph to the database
    graph_to_db(dag_dummy_1, cur)
    conn.commit()

    # Read the graph back from the database
    G_loaded = db_to_graph_from_pip_id(cur, dag_dummy_1.graph['pipeline_id'])

    # Use networkx's is_isomorphic to compare structure and attributes
    def node_match(n1, n2):
        # Compare relevant node attributes
        for attr in ['status', 'editable', 'tag', 'notes', 'folder_path']:
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
    for attr in ['pipeline_id', 'notes', 'tag', 'owner']:
        assert G_loaded.graph[attr] == dag_dummy_1.graph[attr]


def test_graph_dict_to_db_and_db_to_graph_dict_roundtrip(pg_test_db, dict_dummy_1):
    """
    Test that a graph dictionary can be written to the database and then read back as a dictionary,
    and that the structure and attributes are preserved.
    """
    from fusionpipe.utils.pip_utils import graph_dict_to_db, db_to_graph_dict_from_pip_id
    from fusionpipe.utils import db_utils

    # Setup database
    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Write the dummy graph dict to the database
    graph_dict_to_db(dict_dummy_1, cur)
    conn.commit()

    # Read the graph dict back from the database
    loaded_dict = db_to_graph_dict_from_pip_id(cur, dict_dummy_1["pipeline_id"])

    # Check if the loaded dictionary matches the original
    assert loaded_dict == dict_dummy_1, "Loaded graph dict does not match the original graph dict."

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
        for attr in ['status', 'editable', 'tag', 'notes']:
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
    for attr in ['pipeline_id', 'notes', 'tag', 'owner']:
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
    Test that get_all_children_nodes returns all descendants of a node in the pipeline.
    """
    from fusionpipe.utils.pip_utils import get_all_children_nodes, graph_to_db
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    graph_to_db(dag_dummy_1, cur)
    conn.commit()

    # For each node, compare get_all_children_nodes to nx.descendants
    for node in dag_dummy_1.nodes:
        expected = set(nx.descendants(dag_dummy_1, node))
        result = set(get_all_children_nodes(cur, dag_dummy_1.graph['pipeline_id'], node))
        assert result == expected, f"Children nodes for {node} do not match expected descendants."

from conftest import PARENT_NODE_LIST
@pytest.mark.parametrize("start_node", PARENT_NODE_LIST)
def test_branch_pipeline_from_node(pg_test_db, dag_dummy_1, start_node):
    """
    Test that branch_pipeline_from_node creates a new pipeline where all nodes are preserved,
    except the provided node and all its descendants, which are replaced with new IDs.
    """
    from fusionpipe.utils.pip_utils import branch_pipeline_from_node, db_to_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the original graph to the database
    original_graph = dag_dummy_1

    from fusionpipe.utils.pip_utils import graph_to_db

    graph_to_db(original_graph, cur)
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
    new_graph = db_to_graph_from_pip_id(cur, new_pip_id)
    conn.commit()

    # Get descendants of the start_node in the original graph
    descendants = set(nx.descendants(original_graph, start_node))
    nodes_to_replace = descendants | {start_node}
    original_nodes = set(original_graph.nodes)
    new_nodes = set(new_graph.nodes)

    # The new graph should have the same number of nodes as the original
    assert len(new_nodes) == len(original_nodes), "New pipeline should have the same number of nodes as the original."

    # The original graph in the database should not have changed
    original_graph_check = db_to_graph_from_pip_id(cur, original_graph.graph['pipeline_id'])
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

#@pytest.mark.parametrize("start_node", PARENT_NODE_LIST)
def test_duplicate_node_in_pipeline_w_code_and_data(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test that duplicate_node_in_pipeline_w_code_and_data duplicates a node in the pipeline,
    including its code and data folder.
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
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test", owner="tester", notes="test pipeline")
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", editable=1, notes="test node", folder_path=None)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, node_tag="test", position_x=0, position_y=0)
    db_utils.update_folder_path_nodes(cur, node_id, os.path.join(tmp_base_dir, node_id))
    conn.commit()
  
    folder_path_nodes = os.path.join(tmp_base_dir, node_id)
    init_node_folder(folder_path_nodes, verbose=True)


    # Run duplication
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
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id_src, tag="src", owner="tester", notes="src pipeline")
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id_dst, tag="dst", owner="tester", notes="dst pipeline")
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", editable=1, notes="test node", folder_path=None)
    db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id_src, node_tag="test", position_x=0, position_y=0)
    db_utils.update_folder_path_nodes(cur, node_id, os.path.join(tmp_base_dir, node_id))
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
def test_duplicate_duplicate_nodes_in_pipeline_with_relations(monkeypatch, pg_test_db, dag_dummy_1, tmp_base_dir, selected_nodes):
    """
    Test that duplicate_subtree_in_pipeline duplicates a subtree rooted at a node,
    with new node IDs and correct parent-child relations.
    """
    import os
    from fusionpipe.utils.pip_utils import (
        graph_to_db, db_to_graph_from_pip_id, duplicate_nodes_in_pipeline_with_relations, generate_node_id, init_node_folder
    )
    from fusionpipe.utils import db_utils
    import networkx as nx

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    graph_to_db(dag_dummy_1, cur)
    conn.commit()

    pipeline_id = dag_dummy_1.graph['pipeline_id']

    # Setup folders for all nodes to be duplicated
    for node_id in selected_nodes:
        folder_path = os.path.join(tmp_base_dir, node_id)
        init_node_folder(folder_path)
        db_utils.update_folder_path_nodes(cur, node_id, folder_path)
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
    graph = db_to_graph_from_pip_id(cur, pipeline_id)
    for old_parent, old_child in dag_dummy_1.subgraph(selected_nodes).edges:
        assert (id_map[old_parent], id_map[old_child]) in graph.edges

    # Check: original nodes are still present and unchanged
    for node_id in selected_nodes:
        assert node_id in graph.nodes

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
    db_utils.add_node_to_nodes(cur, node_id=node_id, status="ready", editable=1, notes="test node", folder_path=None)
    db_utils.update_folder_path_nodes(cur, node_id, os.path.join(tmp_base_dir, node_id))
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

def test_delete_node_from_pipeline_with_editable_logic(monkeypatch, pg_test_db, tmp_base_dir):
    """
    Test delete_node_from_pipeline_with_editable_logic for editable and non-editable nodes.
    """
    import os
    import shutil
    from fusionpipe.utils.pip_utils import (
        generate_node_id, generate_pip_id, init_node_folder, delete_node_from_pipeline_with_editable_logic
    )
    from fusionpipe.utils import db_utils
    import networkx as nx

    # Patch FUSIONPIPE_DATA_PATH to tmp_base_dir
    monkeypatch.setenv("FUSIONPIPE_DATA_PATH", tmp_base_dir)

    conn = pg_test_db
    cur = db_utils.init_db(conn)
    pipeline_id = generate_pip_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag="test", owner="tester", notes="test pipeline")

    # Case 1: Editable node
    node_id_editable = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id_editable, status="ready", editable=1, notes="editable node", folder_path=None)
    db_utils.add_node_to_pipeline(cur, node_id=node_id_editable, pipeline_id=pipeline_id, node_tag="editable", position_x=0, position_y=0)
    db_utils.update_folder_path_nodes(cur, node_id_editable, os.path.join(tmp_base_dir, node_id_editable))
    conn.commit()
    folder_path_nodes = os.path.join(tmp_base_dir, node_id_editable)
    init_node_folder(folder_path_nodes, verbose=True)
    assert os.path.exists(folder_path_nodes)
    # Should delete node and folder
    delete_node_from_pipeline_with_editable_logic(cur, pipeline_id, node_id_editable)
    conn.commit()
    assert not os.path.exists(folder_path_nodes)
    assert node_id_editable not in db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)

    # Case 2: Non-editable leaf node in non-editable subgraph
    node_id_noneditable = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=node_id_noneditable, status="ready", editable=0, notes="non-editable node", folder_path=None)
    db_utils.add_node_to_pipeline(cur, node_id=node_id_noneditable, pipeline_id=pipeline_id, node_tag="noneditable", position_x=1, position_y=1)
    conn.commit()
    # Should delete node from pipeline (no error)
    delete_node_from_pipeline_with_editable_logic(cur, pipeline_id, node_id_noneditable)
    conn.commit()
    assert node_id_noneditable not in db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)

    # Case 3: Non-editable node that is not a leaf in non-editable subgraph
    parent_id = generate_node_id()
    child_id = generate_node_id()
    db_utils.add_node_to_nodes(cur, node_id=parent_id, status="ready", editable=0, notes="parent", folder_path=None)
    db_utils.add_node_to_nodes(cur, node_id=child_id, status="ready", editable=0, notes="child", folder_path=None)
    db_utils.add_node_to_pipeline(cur, node_id=parent_id, pipeline_id=pipeline_id, node_tag="parent", position_x=2, position_y=2)
    db_utils.add_node_to_pipeline(cur, node_id=child_id, pipeline_id=pipeline_id, node_tag="child", position_x=3, position_y=3)
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
    conn.commit()
    # Should raise ValueError because parent_id is not a leaf
    import pytest
    with pytest.raises(ValueError):
        delete_node_from_pipeline_with_editable_logic(cur, pipeline_id, parent_id)

def test_set_children_stale_sets_descendants_to_staledata(pg_test_db, dag_dummy_1):
    """
    Test that set_children_stale sets all descendants of a node to 'staledata' status.
    """
    from fusionpipe.utils.pip_utils import graph_to_db, set_children_stale
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    graph_to_db(dag_dummy_1, cur)
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
    from fusionpipe.utils.pip_utils import graph_to_db, update_stale_status_for_pipeline_nodes
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Add the dummy graph to the database
    graph_to_db(dag_dummy_1, cur)
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
    - Fails if child is not editable.
    - Fails if adding the edge would create a cycle.
    """
    from fusionpipe.utils.pip_utils import add_node_relation_safe, generate_node_id, generate_pip_id
    from fusionpipe.utils.pip_utils import graph_to_db, db_to_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx
    import pytest

    conn = pg_test_db
    cur = db_utils.init_db(conn)

    # Create a simple pipeline with two nodes
    pipeline_id = generate_pip_id()
    parent_id = generate_node_id()
    child_id = generate_node_id()
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id)
    db_utils.add_node_to_nodes(cur, node_id=parent_id, status="ready", editable=True)
    db_utils.add_node_to_nodes(cur, node_id=child_id, status="ready", editable=True)
    db_utils.add_node_to_pipeline(cur, node_id=parent_id, pipeline_id=pipeline_id)
    db_utils.add_node_to_pipeline(cur, node_id=child_id, pipeline_id=pipeline_id)
    conn.commit()

    # Should succeed: add parent -> child
    assert add_node_relation_safe(cur, pipeline_id, parent_id, child_id) is True
    conn.commit()
    G = db_to_graph_from_pip_id(cur, pipeline_id)
    assert (parent_id, child_id) in G.edges

    # Should fail: adding child -> parent (would create a cycle)
    with pytest.raises(ValueError, match="cycle"):
        add_node_relation_safe(cur, pipeline_id, child_id, parent_id)

    # Should fail: child not editable
    db_utils.update_editable_status(cur, node_id=child_id, editable=False)
    with pytest.raises(ValueError, match="not editable"):
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

    db_utils.add_pipeline(cur, pipeline_id=pipeline_id1)
    db_utils.add_pipeline(cur, pipeline_id=pipeline_id2)
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