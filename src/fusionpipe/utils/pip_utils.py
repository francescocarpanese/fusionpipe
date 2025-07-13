from datetime import datetime
import random
import os
import networkx as nx
import matplotlib.pyplot as plt
import json
from fusionpipe.utils import db_utils
import copy
from enum import Enum
import shutil
import toml
import stat

def change_permissions_recursive(path, mode):
    """Recursively change permissions of a directory and its contents."""
    for root, dirs, files in os.walk(path):
        for d in dirs:
            os.chmod(os.path.join(root, d), mode)
        for f in files:
            os.chmod(os.path.join(root, f), mode)

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

def generate_project_id():
    return "pr_" + generate_id()

def init_node_folder(folder_path_nodes, verbose=False):
    
    # Create the main node folder
    os.makedirs(folder_path_nodes, exist_ok=True)

    code_folder_path = os.path.join(folder_path_nodes,"code")
    node_examples_folder_path = os.path.join(code_folder_path, "examples")
    
    # Create subfolders 'code' and 'data'
    os.makedirs(f"{code_folder_path}", exist_ok=True)
    os.makedirs(node_examples_folder_path, exist_ok=True)
    os.makedirs(f"{folder_path_nodes}/data", exist_ok=True)
    os.makedirs(f"{folder_path_nodes}/reports", exist_ok=True)

    # Add __init__.py files to the examples folder to make it a package
    init_file_path = os.path.join(node_examples_folder_path, '__init__.py')
    if not os.path.exists(init_file_path):
        with open(init_file_path, 'w') as init_file:
            init_file.write("# This file makes the examples folder a package\n")

    # Save the current working directory
    current_dir = os.getcwd()

    try:
        # Run the 'uv init' command inside the node folder
        os.chdir(code_folder_path)
        # Initialize the uv environment with a specific name (if supported)
        env_name = os.path.basename(folder_path_nodes)
        os.system(f"uv init --name {env_name}")

        # Add some package to uv
        os.system("uv add psycopg2-binary")

        # Add the ipykernel package to the virtual environment
        os.system("uv add ipykernel")

        # Add nbconvert for notebook conversion support
        os.system("uv add nbconvert")

        # Install the current environment as a Jupyter kernel
        os.system("uv run python -m ipykernel install --user --name " + env_name + " --display-name " + env_name)

        # Copy the template file into the code folder
        template_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates', 'main_template.py')
        if os.path.exists(template_file_path):
            destination_file_path = os.path.join(code_folder_path, 'main.py')        
            with open(template_file_path, 'r') as template_file:
                with open(destination_file_path, 'w') as dest_file:
                    dest_file.write(template_file.read())
        else:
            raise FileNotFoundError(f"Template file not found at {template_file_path}")
        
        # Copy example files into the code folder
        template_folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
        example_python_path = os.path.join(template_folder_path, 'example_python.py')
        example_matlab_path = os.path.join(template_folder_path, 'example_matlab.m')
        example_notebook_path = os.path.join(template_folder_path, 'example_notebook.ipynb')
        init_node_kernel_path = os.path.join(template_folder_path, 'init_node_kernel.py')
        example_defuse_path = os.path.join(template_folder_path, 'DEFUSE_example.m')
        parmameter_yaml_path = os.path.join(template_folder_path, 'node_parameters.yaml')

        # Copy and grand writing access to the example files
        if os.path.exists(example_defuse_path):
            shutil.copy(example_defuse_path, node_examples_folder_path)
            os.chmod(os.path.join(node_examples_folder_path, "DEFUSE_example.m"), 0o664)

        if os.path.exists(parmameter_yaml_path):
            shutil.copy(parmameter_yaml_path, code_folder_path)
            os.chmod(os.path.join(code_folder_path, "node_parameters.yaml"), 0o664)
        else:
            raise FileNotFoundError(f"Parameters YAML file not found at {parmameter_yaml_path}")

        if os.path.exists(example_python_path):
            shutil.copy(example_python_path, node_examples_folder_path)
            os.chmod(os.path.join(node_examples_folder_path, "example_python.py"), 0o664)
        else:
            raise FileNotFoundError(f"Example Python file not found at {example_python_path}")
        
        if os.path.exists(init_node_kernel_path):
            shutil.copy(init_node_kernel_path, code_folder_path)
            os.chmod(os.path.join(code_folder_path, "init_node_kernel.py"), 0o664)
        else:
            raise FileNotFoundError(f"Example Python file not found at {init_node_kernel_path}")

        if os.path.exists(example_matlab_path):
            shutil.copy(example_matlab_path, node_examples_folder_path)
            os.chmod(os.path.join(node_examples_folder_path, "example_matlab.m"), 0o664)
        else:
            raise FileNotFoundError(f"Example MATLAB file not found at {example_matlab_path}")
        
        if os.path.exists(example_notebook_path):
            shutil.copy(example_notebook_path, node_examples_folder_path)
            os.chmod(os.path.join(node_examples_folder_path, "example_notebook.ipynb"), 0o664)
        else:
            raise FileNotFoundError(f"Example Notebook file not found at {example_notebook_path}")

        # Update the name entry in pyproject.toml using the toml library
        pyproject_file_path = os.path.join(code_folder_path, 'pyproject.toml')
        if os.path.exists(pyproject_file_path):
            with open(pyproject_file_path, 'r') as file:
                pyproject_data = toml.load(file)
                
            # Update the 'name' field
            pyproject_data['project']['name'] = os.path.basename(folder_path_nodes)
            
            # Write the updated data back to the file
            with open(pyproject_file_path, 'w') as file:
                toml.dump(pyproject_data, file)

        # Write the node ID to a .node_id file inside the code folder
        node_id_file_path = os.path.join(code_folder_path, '.node_id')
        with open(node_id_file_path, 'w') as node_id_file:
            node_id_file.write(os.path.basename(folder_path_nodes) + '\n')


    finally:
        # Change back to the previous working directory
        os.chdir(current_dir)


    # Commit all changes in the code folder path
    os.chdir(code_folder_path)
    os.system("git init")  # Initialize a git repository if not already initialized
    os.system("git add .")  # Add all changes to the staging area
    os.system('git commit -m "Initial commit for code folder"')  # Commit the changes
    

    # Run empty main to set-up the .venv
    os.system("uv run")
    os.chdir(current_dir)  # Change back to the original directory    
    
    if verbose:
        print(f"Node folder created at: {folder_path_nodes}")


def delete_node_folder(node_folder_path, verbose=False):
    
    # Remove the node folder and its contents
    if os.path.exists(node_folder_path):
        shutil.rmtree(node_folder_path)
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
        'project_id': graph.graph.get('project_id', ""),  # Optional list of project IDs associated with the pipeline
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
    project_id = graph_dict.get("project_id", "")

    if not db_utils.check_project_exists(cur, project_id):
        # If the project does not exist, create it
        db_utils.add_project(cur, project_id=project_id)

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

        db_utils.add_pipeline_to_project(cur, project_id=project_id, pipeline_id=pipeline_id)
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
        folder_path = Gnx.nodes[node].get('folder_path', None)  # Optional folder path for the node

        # Add the node to the database
        if not db_utils.check_if_node_exists(cur, node):
            db_utils.add_node_to_nodes(cur, node_id=node_id, status=status, editable=editable, notes=node_notes, folder_path=folder_path)
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
    G.graph['project_id'] = db_utils.get_project_id_by_pipeline(cur, pipeline_id=pip_id)  # Optional list of project IDs associated with the pipeline

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
        # If editable, delete the node directly from the pipeline database
        set_children_stale(cur, pipeline_id, node_id)
        db_utils.remove_node_from_pipeline(cur, pipeline_id=pipeline_id, node_id=node_id)
        delete_node_folder(db_utils.get_node_folder_path(cur, node_id=node_id))
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

def get_all_children_nodes(cur, pipeline_id, node_id):
    """
    Get all children of a node in the pipeline.
    """
    graph = db_to_graph_from_pip_id(cur, pipeline_id)
    return list(nx.descendants(graph, node_id))

def copy_with_permissions(src, dst, *, follow_symlinks=True):
    """Custom copy function to ensure destination files are writable."""
    if os.path.isdir(dst):
        dst = os.path.join(dst, os.path.basename(src))
    try:
        shutil.copy2(src, dst, follow_symlinks=follow_symlinks)
    except PermissionError:
        # If we get a permission error, it's likely because the destination
        # file is read-only. Change permissions and try again.
        os.chmod(dst, stat.S_IWUSR)
        shutil.copy2(src, dst, follow_symlinks=follow_symlinks)
    # Ensure the destination file is writable
    os.chmod(dst, os.stat(dst).st_mode | stat.S_IWUSR)
    return dst


def duplicate_node_in_pipeline_w_code_and_data(cur, source_pipeline_id, target_pipeline_id, source_node_id, new_node_id, parents=False, childrens=False, withdata=False):
    """
    Duplicate a node in the pipeline, including its code and data.
    """
    # Duplicate node in the database
    db_utils.duplicate_node_in_pipeline_with_relations(
        cur,
        source_node_id, 
        new_node_id,
        source_pipeline_id,
        target_pipeline_id,
        parents=parents,
        childrens=childrens,
        )

    # New node is 
    db_utils.update_editable_status(cur, node_id=new_node_id, editable=True)

    # Copy the folder into the new node folder
    new_folder_path_nodes = os.path.join(os.environ.get("FUSIONPIPE_DATA_PATH"), new_node_id)
    # Update database
    db_utils.update_folder_path_nodes(cur, new_node_id, new_folder_path_nodes)

    old_folder_path_nodes = db_utils.get_node_folder_path(cur, node_id=source_node_id)

    if old_folder_path_nodes:
        # Ensure the source .git directory is readable before copying
        source_git_dir = os.path.join(old_folder_path_nodes, 'code', '.git')
        if os.path.exists(source_git_dir):
            change_permissions_recursive(source_git_dir, 0o755)

        # Copy only the 'code' and 'reports' subfolders, skipping '.venv'
        os.makedirs(new_folder_path_nodes, exist_ok=True)

        # Initise the new node folder
        init_node_folder(new_folder_path_nodes, verbose=False)

        for subfolder in ['code', 'reports']:
            old_subfolder_path = os.path.join(old_folder_path_nodes, subfolder)
            new_subfolder_path = os.path.join(new_folder_path_nodes, subfolder)
            if os.path.exists(old_subfolder_path):
                # Use ignore to skip .venv if present in subfolder
                shutil.copytree(
                    old_subfolder_path,
                    new_subfolder_path,
                    dirs_exist_ok=True,
                    copy_function=copy_with_permissions,
                    ignore=shutil.ignore_patterns('.venv', '__pycache__', '*.pyc', '*.pyo', '*.pyd', '*.ipynb_checkpoints', '.node_id')
                )

        # Ensure the new .git directory is writable after copying
        new_git_dir = os.path.join(new_folder_path_nodes, 'code', '.git')
        if os.path.exists(new_git_dir):
            change_permissions_recursive(new_git_dir, 0o755)

        if withdata:
            old_data_folder = os.path.join(old_folder_path_nodes, "data")
            new_data_folder = os.path.join(new_folder_path_nodes, "data")
            if os.path.exists(old_data_folder):
                shutil.copytree(old_data_folder, new_data_folder, dirs_exist_ok=True)
            else:
                os.makedirs(new_data_folder, exist_ok=True)
        else:
            os.makedirs(os.path.join(new_folder_path_nodes, "data"), exist_ok=True)

    # Load and update the project.toml file
    project_toml_path = os.path.join(new_folder_path_nodes, "code", "pyproject.toml")
    if os.path.exists(project_toml_path):
        with open(project_toml_path, 'r') as file:
            pyproject_data = toml.load(file)

        # Update the 'name' field with the new node ID
        pyproject_data['project']['name'] = new_node_id

        # Write the updated data back to the file
        with open(project_toml_path, 'w') as file:
            toml.dump(pyproject_data, file)

    # Initialize the .venv using uv
    code_folder_path = os.path.join(new_folder_path_nodes, "code")
    current_dir = os.getcwd()
    try:
        os.chdir(code_folder_path)
        # Set up the virtual environment for the new node.
        os.system("uv run")
        # Add the ipykernel package to the virtual environment of the core user, in order to be able to run the node in Jupyter
        os.system("uv run python -m ipykernel install --user --name " + new_node_id + " --display-name " + new_node_id)

    finally:
        os.chdir(current_dir)

def duplicate_nodes_in_pipeline_with_relations(cur, source_pipeline_id, target_pipeline_id, source_node_ids, withdata=False):
    """
    Duplicate a list of nodes including their relation inside a pipeline.
    """
    if isinstance(source_node_ids, str):
        source_node_ids = [source_node_ids]
    # Get the pipeline graph
    graph = db_to_graph_from_pip_id(cur, source_pipeline_id)
    # Get all nodes in the subtree(s)
    subtree_nodes = set()
    for root in source_node_ids:
        subtree_nodes.add(root)
    # Build subgraph
    subtree = graph.subgraph(subtree_nodes).copy()
    # Map old node ids to new node ids
    id_map = {old_id: generate_node_id() for old_id in subtree.nodes}
    # Duplicate nodes in topological order (parents before children)
    for old_id in nx.topological_sort(subtree):
        new_id = id_map[old_id]
        # Duplicate node in DB (without parents/children relations)
        duplicate_node_in_pipeline_w_code_and_data(
            cur, source_pipeline_id,target_pipeline_id, old_id, new_id, parents=False, childrens=False, withdata=withdata)
    # Set parent-child relations in the duplicated subtree
    for old_parent, old_child in subtree.edges:
        db_utils.add_node_relation(
            cur,
            child_id=id_map[old_child],
            parent_id=id_map[old_parent]
        )

    # Find head nodes of the duplicated subtree (nodes with no incoming edges in the subtree)
    head_nodes = [n for n in subtree.nodes if subtree.in_degree(n) == 0]
    for old_head in head_nodes:
        new_head = id_map[old_head]
        # Find parents of the original head node in the full graph (outside the subtree)
        for parent in graph.predecessors(old_head):
            if parent not in subtree_nodes:
                # Attach the new head node to the parent in the target pipeline
                db_utils.add_node_relation(
                    cur,
                    child_id=new_head,
                    parent_id=parent
                )
    
    # Optionally shift the positions of the duplicated nodes to avoid overlap
    shift_x, shift_y = 40, 40  # You can adjust the shift values as needed
    for old_id, new_id in id_map.items():
        position = db_utils.get_node_position(cur, node_id=old_id, pipeline_id=source_pipeline_id)
        if position is not None:
            new_position = (position[0] + shift_x, position[1] + shift_y)
            db_utils.update_node_position(cur, node_id=new_id, pipeline_id=target_pipeline_id, position_x=new_position[0], position_y=new_position[1])
    
    return id_map

def delete_node_data(cur, node_ids):
    """
    Delete the data associated with node in list
    """
    if isinstance(node_ids, str):
        node_ids = [node_ids]
    
    for node_id in node_ids:
        # Get the folder path of the node
        folder_path = db_utils.get_node_folder_path(cur, node_id=node_id)
        
        if folder_path and os.path.exists(folder_path):
            # Remove only the contents of the "data" folder, not the folder itself
            data_folder = os.path.join(folder_path, "data")
            if os.path.exists(data_folder):
                for entry in os.scandir(data_folder):
                    if entry.is_file() or entry.is_symlink():
                        os.unlink(entry.path)
                    elif entry.is_dir():
                        shutil.rmtree(entry.path, ignore_errors=True)
            print(f"Data for node {node_id} deleted from {folder_path}")

    for node_id in node_ids:
        db_utils.update_node_status(cur, node_id=node_id, status=NodeState.READY.value)

    else:
        print(f"No data found for node {node_id} or folder does not exist.")

def set_children_stale(cur, pipeline_id, node_id):
    """
    Set the status of all children (descendants) of a node to 'staledata'.
    """
    children = get_all_children_nodes(cur, pipeline_id, node_id)
    for child_id in children:
        db_utils.update_node_status(cur, node_id=child_id, status=NodeState.STALEDATA.value)

def update_stale_status_for_pipeline_nodes(cur, pipeline_id):
    """
    Propagate 'staledata' status through the pipeline graph:
    If a node has any parent in 'staledata' or 'failed', set its status to 'staledata'.
    This is done recursively using the NetworkX graph structure.
    """
    # Build the graph from the database
    G = db_to_graph_from_pip_id(cur, pipeline_id)
    # Traverse in topological order (parents before children)
    for node_id in nx.topological_sort(G):
        # Check all parents
        for parent_id in G.predecessors(node_id):
            parent_status = G.nodes[parent_id]["status"]
            if parent_status in [NodeState.STALEDATA.value, NodeState.FAILED.value, NodeState.READY.value]:
                if G.nodes[node_id]["status"] not in [NodeState.READY.value]:
                    # If the node is not already in 'staledata' or 'failed', update it
                    db_utils.update_node_status(cur, node_id, NodeState.STALEDATA.value)
                    G.nodes[node_id]["status"] = NodeState.STALEDATA.value

def delete_edge_and_update_status(cur, pipeline_id, parent_id, child_id):
    # Check if the child node is editable
    if not db_utils.is_node_editable(cur, node_id=child_id):
        raise ValueError(f"Child node {child_id} is not editable. You cannot delete edges from it. Consider duplicating the node if you want to branch the pipeline")

    # Set all descendants of the child node to 'staledata'
    set_children_stale(cur, pipeline_id, parent_id)

    # Remove the edge and update the pipeline using editable logic
    db_utils.remove_node_relation_with_editable_logic(cur, parent_id=parent_id, child_id=child_id)

def add_node_relation_safe(cur, pipeline_id, parent_id, child_id):
    """
    Safely add a node relation (edge) to the pipeline graph.
    - Checks that the child node is editable.
    - Checks that adding the edge does not create a cycle.
    - Adds the relation if all checks pass.
    """
    from fusionpipe.utils import db_utils
    import networkx as nx

    # Get the current pipeline graph
    graph = db_to_graph_from_pip_id(cur, pipeline_id)

    # Check if the child node is editable
    if not db_utils.is_node_editable(cur, node_id=child_id):
        raise ValueError(f"Child node {child_id} is not editable. Cannot add relation.")

    # Check if adding the edge would create a cycle
    temp_graph = graph.copy()
    temp_graph.add_edge(parent_id, child_id)
    if not nx.is_directed_acyclic_graph(temp_graph):
        raise ValueError(f"Adding edge from {parent_id} to {child_id} would create a cycle.")
    
    # If the child node is in 'completed' status, set it to 'staledata'
    child_status = db_utils.get_node_status(cur, node_id=child_id)
    if child_status == NodeState.COMPLETED.value:
        db_utils.update_node_status(cur, node_id=child_id, status=NodeState.STALEDATA.value)

    # Add the relation
    db_utils.add_node_relation(cur, child_id=child_id, parent_id=parent_id)

    # Refresh the status of the child node in the pipeline after adding the relation
    update_stale_status_for_pipeline_nodes(cur, pipeline_id)

    return True

def merge_pipelines(cur, source_pipeline_ids):
    """
    Merge multiple pipelines into a new pipeline by copying all their nodes.
    Duplicate nodes are copied only once.
    :param cur: Database cursor
    :param source_pipeline_ids: List of pipeline IDs to merge
    :return: The new target pipeline ID
    """
    # Generate a new pipeline ID
    target_pipeline_id = generate_pip_id()
    # Create the new pipeline in the database
    db_utils.add_pipeline(cur, pipeline_id=target_pipeline_id, tag=target_pipeline_id)

    # Keep track of already duplicated nodes
    duplicated_nodes = set()

    # Loop through all source pipelines
    for source_pipeline_id in source_pipeline_ids:
        # Get all node IDs from the source pipeline
        node_ids = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id=source_pipeline_id)
        for node_id in node_ids:
            if node_id not in duplicated_nodes:
                db_utils.duplicate_node_pipeline_relation(cur, source_pipeline_id, node_id, target_pipeline_id)
        duplicated_nodes.update(node_ids)


    return target_pipeline_id
