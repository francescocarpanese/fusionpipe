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


def test_init_node_folder(tmp_base_dir):
    # Test if the function creates the expected node folder structure
    from fusionpipe.utils.pip_utils import init_node_folder
    import os

    node_id = "test_node"
    base_path = tmp_base_dir
    init_node_folder(base_path, verbose=True)

    node_folder = os.path.join(base_path)
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


def test_graph_to_db_and_db_to_graph_roundtrip(in_memory_db_conn, dag_dummy_1):
    """
    Test that a graph can be written to the database and then read back,
    and that the structure and attributes are preserved.
    """
    from fusionpipe.utils.pip_utils import graph_to_db, db_to_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx

    # Setup database
    conn = in_memory_db_conn
    cur = db_utils.init_db(conn)

    # Write the dummy graph to the database
    graph_to_db(dag_dummy_1, cur)
    conn.commit()

    # Read the graph back from the database
    G_loaded = db_to_graph_from_pip_id(cur, dag_dummy_1.graph['pipeline_id'])

    # Use networkx's is_isomorphic to compare structure and attributes
    def node_match(n1, n2):
        # Compare relevant node attributes
        for attr in ['status', 'editable', 'tag', 'notes']:
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


def test_graph_dict_to_db_and_db_to_graph_dict_roundtrip(in_memory_db_conn, dict_dummy_1):
    """
    Test that a graph dictionary can be written to the database and then read back as a dictionary,
    and that the structure and attributes are preserved.
    """
    from fusionpipe.utils.pip_utils import graph_dict_to_db, db_to_graph_dict_from_pip_id
    from fusionpipe.utils import db_utils

    # Setup database
    conn = in_memory_db_conn
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

def test_get_all_children_nodes(in_memory_db_conn, dag_dummy_1):
    """
    Test that get_all_children_nodes returns all descendants of a node in the pipeline.
    """
    from fusionpipe.utils.pip_utils import get_all_children_nodes, graph_to_db
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = in_memory_db_conn
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
def test_branch_pipeline_from_node(in_memory_db_conn, dag_dummy_1, start_node):
    """
    Test that branch_pipeline_from_node creates a new pipeline where all nodes are preserved,
    except the provided node and all its descendants, which are replaced with new IDs.
    """
    from fusionpipe.utils.pip_utils import branch_pipeline_from_node, db_to_graph_from_pip_id
    from fusionpipe.utils import db_utils
    import networkx as nx

    conn = in_memory_db_conn
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


