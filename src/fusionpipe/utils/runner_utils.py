#import ray
import subprocess
import os
import time
from fusionpipe.utils import db_utils

#ray.init(ignore_reinit_error=True)

#@ray.remote
def run_node(node_id, db_path):
    # Run the node's run.py as a subprocess
    try:
        conn = db_utils.connect_to_db(db_path)
        cur = conn.cursor()
        node_path = db_utils.get_node_folder_path(cur, node_id)
        
        db_utils.update_node_status(cur, node_id, 'running')
        conn.commit()
        result = subprocess.run(['python', os.path.join(node_path, 'code','run.py')], check=True)
        # Update status to completed
        db_utils.update_node_status(cur, node_id, 'completed')
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        # Update status to failed
        conn = db_utils.connect_to_db(db_path)
        cur = conn.cursor()
        db_utils.update_node_status(cur, node_id, 'failed')
        conn.commit()
        conn.close()
        return False

def can_run(cur, node_id):
    # All parents must be completed
    parents = db_utils.get_node_parents(cur, node_id)
    return all(db_utils.get_node_status(cur, pid) == 'completed' for pid in parents)

# def orchestrate_pipeline(db_path, pipeline_id, node_paths, debug=False):
#     conn = db_utils.connect_to_db(db_path)
#     cur = conn.cursor()
#     all_nodes = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)
#     running = {}
#     completed = set()
#     failed = set()

#     while True:
#         progress = False
#         for node_id in all_nodes:
#             status = db_utils.get_node_status(cur, node_id)
#             if status == 'completed':
#                 completed.add(node_id)
#                 continue
#             if status == 'failed':
#                 failed.add(node_id)
#                 continue
#             if node_id in running:
#                 continue
#             if can_run(cur, node_id):
#                 # Schedule node
#                 #node_path = node_paths[node_id]
#                 running[node_id] = run_node(node_id, db_path)
#                 progress = True

#         # Check for finished nodes
#         finished = []
#         for node_id, obj_ref in running.items():
#             if ray.get(obj_ref):
#                 finished.append(node_id)
#         for node_id in finished:
#             running.pop(node_id)

#         if len(completed) + len(failed) == len(all_nodes):
#             break
#         if not progress and not running:
#             # No progress and nothing running: deadlock or done
#             break
#         time.sleep(1)
#     conn.close()


# Applying ray will be done later
db_path = "/misc/carpanes/fusionpipe/bin/pipeline.db"
pipeline_id = "p_20250610190444_4358"

conn = db_utils.connect_to_db(db_path)
cur = conn.cursor()
all_nodes = db_utils.get_all_nodes_from_pip_id(cur, pipeline_id)
running = {}
completed = set()
failed = set()


# Define color mapping based on node status
color_map = {
    "ready": "gray",
    "failed": "red",
    "completed": "green",
    "running": "blue",
    "staledata": "yellow"
}

node_id = "n_20250610190446_4500"
run_node(node_id, db_path)

# while True:
#     progress = False
#     for node_id in all_nodes:
#         status = db_utils.get_node_status(cur, node_id)
#         if status == 'completed':
#             completed.add(node_id)
#             continue
#         if status == 'failed':
#             failed.add(node_id)
#             continue
#         if node_id in running:
#             continue
#         if can_run(cur, node_id):
#             # Schedule node
#             running[node_id] = run_node(node_id, db_path)
#             progress = True

#     # Check for finished nodes
#     finished = []
#     for node_id, obj_ref in running.items():
#         if ray.get(obj_ref):
#             finished.append(node_id)
#     for node_id in finished:
#         running.pop(node_id)

#     if len(completed) + len(failed) == len(all_nodes):
#         break
#     if not progress and not running:
#         # No progress and nothing running: deadlock or done
#         break
#     time.sleep(1)
# conn.close()