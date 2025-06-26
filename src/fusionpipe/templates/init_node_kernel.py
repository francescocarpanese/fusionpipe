# Initialise the Jupyter kernel for the node.
import subprocess
import os
import sys
sys.path.insert(0, os.environ.get("USER_UTILS_FOLDER_PATH"))
from python_user_utils.node_api import get_all_parent_node_folder_paths, get_node_id, get_folder_path_data, get_folder_path_reports

node_id = get_node_id()
cmd = [
    "uv", "run", "python", "-m", "ipykernel", "install",
    "--user",
    "--name", node_id,
    "--display-name", node_id,
]

subprocess.run(cmd, check=True, cwd=os.path.dirname(__file__))
