from fusionpipe.utils import db_utils

node_id = "n_20250621215610_3296"

conn = db_utils.connect_to_db()
cur = conn.cursor()

node_parents = db_utils.get_node_parents(cur, node_id)

parent = node_parents[0]

folder_parent = db_utils.get_node_folder_path(cur, parent)