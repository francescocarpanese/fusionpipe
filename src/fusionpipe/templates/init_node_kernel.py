# Initialise the Jupyter kernel for the node.
import subprocess
import os
import sys
from fp_user_utils.user_api import get_current_node_id

node_id = get_current_node_id()
cmd = [
    "uv", "run", "python", "-m", "ipykernel", "install",
    "--user",
    "--name", node_id,
    "--display-name", node_id,
]

subprocess.run(cmd, check=True, cwd=os.path.dirname(__file__))
