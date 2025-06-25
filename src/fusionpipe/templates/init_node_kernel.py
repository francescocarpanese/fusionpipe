# Initialise the Jupyter kernel for the node.
from user_utils.python.node_api import get_node_id
import subprocess
import os

node_id = get_node_id()
cmd = [
    "uv", "run", "python", "-m", "ipykernel", "install",
    "--user",
    "--name", node_id,
    "--display-name", node_id,
]

subprocess.run(cmd, check=True, cwd=os.path.dirname(__file__))
