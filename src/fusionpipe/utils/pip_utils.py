from datetime import datetime
import random
import os
import networkx as nx
import matplotlib.pyplot as plt
import json
from fusionpipe.utils import db_utils
import copy
from enum import Enum

class NodeState(Enum):
    READY = "ready"       # Node is created but not yet processed
    RUNNING = "running"       # Node is currently being processed
    COMPLETED = "completed"   # Node has finished processing successfully
    FAILED = "failed"         # Node processing failed
    STALEDATA = "staledata"           # Node is outdated or no longer valid


def generate_id():
    # Get the current date and time
    now = datetime.now()
    # Format the identifier as YYYYMMDDHHMMSS + random 4-digit number
    unique_id = now.strftime("%Y%m%d%H%M%S") + "_" +  f"{random.randint(1000, 9999):04d}"
    return unique_id

def generate_node_id():
    return "n_" + generate_id()

def generate_pip_id():
    return "p_" + generate_id()

def init_node_folder(node_folder_path, verbose=False):
    
    # Create the main node folder
    os.makedirs(node_folder_path, exist_ok=True)
    
    # Create subfolders 'code' and 'data'
    os.makedirs(f"{node_folder_path}/code", exist_ok=True)
    os.makedirs(f"{node_folder_path}/data", exist_ok=True)
    os.makedirs(f"{node_folder_path}/reports", exist_ok=True)

    # Copy the template file into the code folder
    template_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates', 'run.py')
    if os.path.exists(template_file_path):
        destination_file_path = os.path.join(node_folder_path, 'code', 'run.py')
        with open(template_file_path, 'r') as template_file:
            with open(destination_file_path, 'w') as dest_file:
                dest_file.write(template_file.read())
    else:
        raise FileNotFoundError(f"Template file not found at {template_file_path}")
    
    if verbose:
        print(f"Node folder created at: {node_folder_path}")

def delete_node_folder(node_folder_path, node_id, verbose=False):
    
    # Construct the node folder path
    node_folder_path = f"{node_folder_path}/n_{node_id}"
    
    # Remove the node folder and its contents
    if os.path.exists(node_folder_path):
        os.rmdir(node_folder_path)
        if verbose:
            print(f"Node folder deleted: {node_folder_path}")
    else:
        if verbose:
            print(f"Node folder does not exist: {node_folder_path}")


def graph_to_dict(graph):
    """
    Convert a NetworkX directed graph to a dictionary format suitable for serialization.
    """

    pipeline_data = {
        'pipeline_id': graph.graph.get('pipeline_id'), # This is the tag that can be given to the pipeline
        'nodes': {},
        'notes': graph.graph.get('notes', None),  # Optional notes for the pipeline
        'tag': graph.graph.get('tag', None),  # Optional tag for the pipeline
        'owner': graph.graph.get('owner', None),  # Optional owner for the pipeline
    }
    for node in graph.nodes:
        parents = list(graph.predecessors(node))
        pipeline_data['nodes'][node] = {
            'parents': parents,
            'status': graph.nodes[node].get('status', 'null'),  # Default status is 'null'
            'editable': graph.nodes[node].get('editable', True),  # Default editable is True
            'tag': graph.nodes[node].get('tag', None),  # Optional tag for the node
            'notes': graph.nodes[node].get('notes', None),  # Optional notes for the node
            'position': graph.nodes[node].get('position', None),  # Node position
            'folder_path': graph.nodes[node].get('folder_path', None)  # Optional folder path for the node
        }
    return pipeline_data

def graph_dict_to_json(graph_dict, file_path, verbose=False):
    """
    Serialize a graph dictionary to a JSON file.
    """
    with open(file_path, 'w') as f:
        json.dump(graph_dict, f, indent=2)

    if verbose:
        print(f"Graph data saved to {file_path}")

def graph_dict_to_db(graph_dict, cur):
    # Insert pipeline using graph_dict fields
    pipeline_id = graph_dict["pipeline_id"]
    tag = graph_dict["tag"]
    owner = graph_dict["owner"]
    notes = graph_dict["notes"]

    if not db_utils.check_pipeline_exists(cur, pipeline_id):
        # If the pipeline does not exist, create it
        db_utils.add_pipeline(cur, pipeline_id=pipeline_id, tag=tag, owner=owner, notes=notes)


        # Insert nodes using graph_dict fields
        for node_id, node_data in graph_dict["nodes"].items():
            status = node_data["status"]
            editable = int(node_data["editable"])
            node_notes = node_data["notes"]
            node_tag = node_data["tag"]
            position = node_data.get("position", None)
            folder_path = node_data.get("folder_path", None)

            db_utils.add_node_to_nodes(cur, node_id=node_id, status=status, editable=editable, notes=node_notes,folder_path=folder_path)
            db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pipeline_id, node_tag=node_tag, position_x=position[0], position_y=position[1])

        # Insert node relations (edges) using parents field
        for child_id, node_data in graph_dict["nodes"].items():
            for parent_id in node_data.get("parents", []):
                db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id)
        return cur

def graph_to_db(Gnx, cur):
    """
    - This function can be used to add a graph to the database.
    - This will also work to add a subgraph to an existing pipeline.
    """
    # Add the pipeline to the database
    pip_id = Gnx.graph['pipeline_id']
    graph_tag = Gnx.graph.get('tag', None)
    owner = Gnx.graph.get('owner', None)
    notes = Gnx.graph.get('notes', None)

    # Check if the pipeline already exists, if not, add it
    if not db_utils.check_pipeline_exists(cur, pip_id):
        db_utils.add_pipeline(cur, pipeline_id=pip_id, tag=graph_tag, owner=owner, notes=notes)

    # Add nodes and their dependencies directly from the graph
    for node in Gnx.nodes:
        node_id = node
        status = Gnx.nodes[node].get('status', 'ready')  # Default status is 'ready'
        editable = Gnx.nodes[node].get('editable', True)  # Default editable is True
        node_notes = Gnx.nodes[node].get('notes', "")  # Optional notes for the node
        node_tag = Gnx.nodes[node].get('tag', None)  # Optional tag for the node
        position = Gnx.nodes[node].get('position', None)  # Node position

        db_utils.get_all_nodes_from_nodes(cur)
        db_utils.get_node_parents(cur, node_id=node_id)


        # Add the node to the database
        if not db_utils.check_if_node_exists(cur, node):
            db_utils.add_node_to_nodes(cur, node_id=node_id, status=status, editable=editable, notes=node_notes)
            # If node already existed, parants cannot have change. Children can and will be added later
            for parent in Gnx.predecessors(node):
                db_utils.add_node_relation(cur, child_id=node_id, parent_id=parent)
        db_utils.add_node_to_pipeline(cur, node_id=node_id, pipeline_id=pip_id, node_tag=node_tag, 
                                     position_x=position[0], position_y=position[1])

def db_to_graph_from_pip_id(cur, pip_id):
    # Load the pipeline from the database
    if not db_utils.check_pipeline_exists(cur, pip_id):
        raise ValueError(f"Pipeline with ID {pip_id} does not exist in the database.")
 
    # Create a directed graph
    G = nx.DiGraph()
    G.graph['pipeline_id'] = pip_id  # Set the graph ID from the pipeline data
    G.graph['notes'] = db_utils.get_pipeline_notes(cur, pipeline_id=pip_id)  # Optional notes for the pipeline
    G.graph['tag'] = db_utils.get_pipeline_tag(cur, pipeline_id=pip_id)  # Set the graph tag from the pipeline data
    G.graph['owner'] = db_utils.get_pipeline_owner(cur, pipeline_id=pip_id)  # Optional owner for the pipeline

    # Add nodes and their dependencies
    for node_id in db_utils.get_all_nodes_from_pip_id(cur, pipeline_id=pip_id):
        G.add_node(node_id)  # Add node with attributes
        # Get the status of the node from the database and add as an attribute
        status = db_utils.get_node_status(cur, node_id=node_id)
        if status not in NodeState._value2member_map_:
            raise ValueError(f"Invalid status '{status}' for node {node_id}. Must be one of {list(NodeState._value2member_map_.keys())}.")
        editable = db_utils.is_node_editable(cur, node_id=node_id)
        G.nodes[node_id]['status'] = status
        G.nodes[node_id]['editable'] = editable
        G.nodes[node_id]['notes'] = db_utils.get_node_notes(cur, node_id=node_id)
        G.nodes[node_id]['folder_path'] = db_utils.get_node_folder_path(cur, node_id=node_id)

        # Add position if available
        position = db_utils.get_node_position(cur, node_id=node_id, pipeline_id=pip_id)
        if position:
            G.nodes[node_id]['position'] = position

        # Add edges based on parent relationships
        for parent_id in db_utils.get_node_parents(cur, node_id=node_id):
            G.add_edge(parent_id, node_id)
    
        # Add node tag if it exists
        tag = db_utils.get_node_tag(cur, node_id=node_id, pipeline_id=pip_id)
        if tag:
            G.nodes[node_id]['tag'] = tag
        else:
            G.nodes[node_id]['tag'] = ""

    return G

def db_to_graph_dict_from_pip_id(cur, pip_id):
    graph = db_to_graph_from_pip_id(cur, pip_id)
    return graph_to_dict(graph)


def dict_to_graph(graph_dict):
    """
    Convert a dictionary representation of a graph back to a NetworkX directed graph.
    """
    G = nx.DiGraph()
    
    # Set graph attributes
    G.graph['pipeline_id'] = graph_dict.get('pipeline_id', None)
    G.graph['notes'] = graph_dict.get('notes', None)
    G.graph['tag'] = graph_dict.get('tag', None)
    G.graph['owner'] = graph_dict.get('owner', None)

    # Add nodes and their attributes
    for node_id, node_data in graph_dict['nodes'].items():
        G.add_node(node_id, status=node_data.get('status', 'ready'), editable=node_data.get('editable', True),
                   tag=node_data.get('tag', None), notes=node_data.get('notes', None), folder_path=node_data.get('folder_path', None))
        for parent in node_data.get('parents', []):
            G.add_edge(parent, node_id)

    return G

def visualize_pip_static(graph):
    plt.figure(figsize=(10, 6))
    pos = nx.spring_layout(graph)

    # Define color mapping based on node status
    color_map = {
        "ready": "gray",
        "failed": "red",
        "completed": "green",
        "running": "blue",
        "staledata": "yellow"
    }

    # Get node colors based on their status
    node_colors = [
        color_map.get(graph.nodes[node].get("status", "ready"), "gray")
        for node in graph.nodes
    ]

    nx.draw(
        graph,
        pos,
        with_labels=True,
        node_color=node_colors,
        node_size=2000,
        font_size=10
    )
    plt.title(f"Pipeline: {graph.name}")
    plt.show()

def generate_data_folder(base_path):
    os.makedirs(os.path.join(base_path, "nodes"), exist_ok=True)
    
    return {
        "nodes": os.path.join(base_path, "nodes"),
    }


def visualize_pip_interactive(graph, output_file="pipeline_visualization.html"):
    from pyvis.network import Network 
    """
    Visualize the pipeline graph interactively using pyvis.
    """
    # Create a new Network object
    net = Network(height="750px", width="100%", directed=True)
    
    # Define color mapping based on node status
    color_map = {
        "ready": "gray",
        "failed": "red",
        "completed": "green",
        "running": "blue",
        "staledata": "yellow"
    }

    # Add nodes with attributes
    for node in graph.nodes:
        status = graph.nodes[node].get("status", "ready")
        color = color_map.get(status, "gray")
        title = " ".join([f"{key}: {value}" for key, value in graph.nodes[node].items()])
        net.add_node(node, label=node, color=color, title=title)

    # Add edges
    for edge in graph.edges:
        net.add_edge(edge[0], edge[1])

    # Generate and save the interactive visualization
    net.write_html(output_file)


def branch_pipeline_from_node(cur, pipeline_id, node_id):
    """
    Branch a pipeline from a node means:
    - Copy the pipeline.
    - All nodes are preserved, except the specified node and all its descendants,
      for which new nodes are created (with new IDs).
    """
    from fusionpipe.utils import db_utils
    # Get the original graph from the database
    original_graph = db_to_graph_from_pip_id(cur, pipeline_id)
    
    # Generate a new pipeline ID and tag
    new_pip_id = generate_pip_id()

    # Find all descendants of the specified node (children, grandchildren, etc.)
    descendants = nx.descendants(original_graph, node_id)

    # Include the node itself
    nodes_to_replace = set(descendants) | {node_id}

    # Map old node IDs to new node IDs for replaced nodes
    id_map = {old_id: generate_node_id() for old_id in nodes_to_replace}

    # Create a new graph for the new pipeline
    new_graph = nx.DiGraph()
    new_graph.graph.update(original_graph.graph)
    new_graph.graph['pipeline_id'] = new_pip_id
    new_graph.graph['tag'] = new_pip_id

    # Add nodes: copy all nodes, but for nodes_to_replace use new IDs
    for n in original_graph.nodes:
        if n in nodes_to_replace:
            new_id = id_map[n]
            attrs = original_graph.nodes[n].copy()
            new_graph.add_node(new_id, **attrs)
        else:
            attrs = original_graph.nodes[n].copy()
            new_graph.add_node(n, **attrs)

    # Add edges: remap edges for replaced nodes
    for u, v in original_graph.edges:
        u_new = id_map[u] if u in nodes_to_replace else u
        v_new = id_map[v] if v in nodes_to_replace else v
        new_graph.add_edge(u_new, v_new)

    # Add the new graph to the database
    graph_to_db(new_graph, cur)

    return new_pip_id


def delete_node_from_pipeline_with_editable_logic(cur,pipeline_id, node_id):
    # Check if the node is editable
    if db_utils.is_node_editable(cur, node_id=node_id):
        # If not editable, delete the node directly
        db_utils.remove_node_from_pipeline(cur, pipeline_id=pipeline_id, node_id=node_id)
        return

    # Get the pipeline graph from the database
    graph = db_to_graph_from_pip_id(cur, pipeline_id)

    # Get subgraph of non-editable nodes
    non_editable_nodes = [n for n in graph.nodes if not graph.nodes[n].get('editable', True)]
    subgraph = graph.subgraph(non_editable_nodes)

    # Check if the node is a leaf (no children)
    if list(subgraph.successors(node_id)):
        raise ValueError(f"Node {node_id} is not a leaf of non editable subgraph.")

    # Delete the node from the pipeline
    db_utils.remove_node_from_pipeline(cur, pipeline_id=pipeline_id, node_id=node_id)


def can_node_run(cur, node_id):
    # Check if the node is in 'ready' state
    status = db_utils.get_node_status(cur, node_id=node_id)
    if status != NodeState.READY.value:
        return False

    # Get all parent nodes
    parent_ids = db_utils.get_node_parents(cur, node_id=node_id)
    # Check if all parents are in 'completed' state
    for parent_id in parent_ids:
        parent_status = db_utils.get_node_status(cur, node_id=parent_id)
        if parent_status != NodeState.COMPLETED.value:
            return False
    # If node has no parents, it can run
    return True

