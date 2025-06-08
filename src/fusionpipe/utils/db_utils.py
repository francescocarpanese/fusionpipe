import sqlite3

table_names = [
    'pipelines',
    'nodes',
    'node_pipeline_relation',
    'node_relation',
    'pipeline_description'
]

def load_db(db_path='pipeline.db'):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn

def connect_to_db(db_path='pipeline.db'):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn

def create_db(db_path='pipeline.db'):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    init_db(conn)
    return conn

def init_db(conn):
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS pipelines (
            pipeline_id TEXT PRIMARY KEY,
            tag TEXT DEFAULT NULL,
            owner TEXT DEFAULT NULL,
            notes TEXT DEFAULT NULL
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,
            status TEXT CHECK(status IN ('ready', 'running', 'completed', 'failed', 'staledata')) DEFAULT 'ready',
            editable BOOLEAN DEFAULT TRUE,
            notes TEXT DEFAULT NULL
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS node_pipeline_relation (
            node_id TEXT,
            pipeline_id TEXT,                
            last_update timestamp DEFAULT CURRENT_TIMESTAMP,
            user TEXT,
            node_tag TEXT,
            FOREIGN KEY (node_id) REFERENCES nodes(id),
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(id),
            PRIMARY KEY (node_id, pipeline_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS node_relation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            child_id TEXT,
            parent_id TEXT,
            FOREIGN KEY (child_id) REFERENCES nodes(id),
            FOREIGN KEY (parent_id) REFERENCES nodes(id)
        )
    ''')
    
    conn.commit()
    return cur


def add_pipeline(cur, pipeline_id, tag=None, owner=None, notes=None):
    cur.execute('INSERT INTO pipelines (pipeline_id, tag, owner, notes) VALUES (?, ?, ?, ?)', (pipeline_id, tag, owner, notes))
    return cur.lastrowid

def add_node_to_nodes(cur, node_id, status='ready', editable=True, notes=None):
    cur.execute('INSERT INTO nodes (node_id, status, editable, notes) VALUES (?, ?, ?, ?)', 
                (node_id, status, editable, notes))
    return cur.lastrowid

def remove_node_from_nodes(cur, node_id):
    cur.execute('DELETE FROM nodes WHERE node_id = ?', (node_id,))
    return cur.rowcount

def get_node_tag(cur, pipeline_id, node_id):
    cur.execute('SELECT node_tag FROM node_pipeline_relation WHERE pipeline_id = ? AND node_id = ?', (pipeline_id, node_id))
    row = cur.fetchone()
    return row[0] if row else None

def add_node_to_pipeline(cur, node_id, pipeline_id, user=None, node_tag=None):
    if not node_tag:
        node_tag = node_id
    cur.execute('INSERT INTO node_pipeline_relation (node_id, pipeline_id, user, node_tag) VALUES (?, ?, ?, ?)', (node_id, pipeline_id, user, node_tag))
    
    # Check if the node is present in more than one pipeline
    cur.execute('SELECT COUNT(*) FROM node_pipeline_relation WHERE node_id = ?', (node_id,))
    count = cur.fetchone()[0]

    # If the node is present in more than one pipeline, set editable to false
    if count > 1:
        cur.execute('UPDATE nodes SET editable = FALSE WHERE node_id = ?', (node_id,))
    
    return cur.lastrowid

def add_node_relation(cur, child_id, parent_id):
    cur.execute('INSERT INTO node_relation (child_id, parent_id) VALUES (?, ?)', (child_id, parent_id))
    return cur.lastrowid

def get_node_parents(cur, node_id):
    cur.execute('SELECT parent_id FROM node_relation WHERE child_id = ?', (node_id,))
    return [row[0] for row in cur.fetchall()]

def get_node_children(cur, node_id):
    cur.execute('SELECT child_id FROM node_relation WHERE parent_id = ?', (node_id,))
    return [row[0] for row in cur.fetchall()]

def update_node_status(cur, node_id, status):
    cur.execute('UPDATE nodes SET status = ? WHERE node_id = ?', (status, node_id))
    return cur.rowcount

def update_node_tag(cur, pipeline_id, node_id, node_tag):
    cur.execute('UPDATE node_pipeline_relation SET node_tag = ? WHERE pipeline_id = ? AND node_id = ?', (node_tag, pipeline_id, node_id))
    return cur.rowcount

def get_node_status(cur, node_id):
    cur.execute('SELECT status FROM nodes WHERE node_id = ?', (node_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_pipeline_tag(cur, pipeline_id):
    cur.execute('SELECT tag FROM pipelines WHERE pipeline_id = ?', (pipeline_id,))
    row = cur.fetchone()
    return row[0] if row else None

def check_pipeline_exists(cur, pipeline_id):
    cur.execute('SELECT 1 FROM pipelines WHERE pipeline_id = ?', (pipeline_id,))
    return cur.fetchone() is not None

def get_all_nodes_from_pip_id(cur, pipeline_id):
    cur.execute('SELECT node_id FROM node_pipeline_relation WHERE pipeline_id = ?', (pipeline_id,))
    return [row[0] for row in cur.fetchall()]

def get_all_nodes_from_nodes(cur):
    cur.execute('SELECT node_id FROM nodes')
    return [row[0] for row in cur.fetchall()]

def check_if_node_exists(cur, node_id):
    cur.execute('SELECT 1 FROM nodes WHERE node_id = ?', (node_id,))
    return cur.fetchone() is not None

def get_nodes_without_pipeline(cur):
    cur.execute('''
        SELECT node_id FROM nodes
        WHERE node_id NOT IN (SELECT node_id FROM node_pipeline_relation)
    ''')
    return [row[0] for row in cur.fetchall()]

def clear_database(cur):
    cur.execute('DELETE FROM pipelines')
    cur.execute('DELETE FROM nodes')
    cur.execute('DELETE FROM node_pipeline_relation')
    cur.execute('DELETE FROM node_relation')
    cur.execute('DELETE FROM pipeline_description')
    return cur

def remove_node_from_relations(cur, node_id):
    cur.execute('DELETE FROM node_relation WHERE child_id = ? OR parent_id = ?', (node_id, node_id))
    return cur.rowcount

def remove_node_from_node_pipeline_relation(cur, node_id):
    cur.execute('DELETE FROM node_pipeline_relation WHERE node_id = ?', (node_id,))
    return cur.rowcount

def remove_node_from_everywhere(cur, node_id):
    remove_node_from_node_pipeline_relation(cur, node_id)
    remove_node_from_relations(cur, node_id)
    remove_node_from_nodes(cur, node_id)
    return cur.rowcount

def get_rows_with_node_id_in_entries(cur, node_id):
    cur.execute('SELECT * FROM node_pipeline_relation WHERE node_id = ?', (node_id,))
    return cur.fetchall()

def get_rows_node_id_in_nodes(cur, node_id):
    cur.execute('SELECT * FROM nodes WHERE node_id = ?', (node_id,))
    return cur.fetchall()

def get_rows_with_node_id_relations(cur, node_id):
    cur.execute('SELECT * FROM node_relation WHERE child_id = ? OR parent_id = ?', (node_id, node_id))
    return cur.fetchall()

def get_rows_with_pipeline_id_in_entries(cur, pipeline_id):
    cur.execute('SELECT * FROM node_pipeline_relation WHERE pipeline_id = ?', (pipeline_id,))
    return cur.fetchall()

def get_rows_with_pipeline_id_in_pipelines(cur, pipeline_id):
    cur.execute('SELECT * FROM pipelines WHERE pipeline_id = ?', (pipeline_id,))
    return cur.fetchall()

def remove_node_from_pipeline(cur, node_id, pipeline_id):
    cur.execute('DELETE FROM node_pipeline_relation WHERE node_id = ? AND pipeline_id = ?', (node_id,pipeline_id))
    # Make node editable if present in only 1 pipeline.
    cur.execute('SELECT COUNT(*) FROM node_pipeline_relation WHERE node_id = ?', (node_id,))
    count = cur.fetchone()[0]
    if count <= 1:
        cur.execute('UPDATE nodes SET editable = TRUE WHERE node_id = ?', (node_id,))
    sanitize_node_relation(cur, pipeline_id)
    return cur.rowcount    

def duplicate_pipeline_in_pipelines(cur, source_pipeline_id, new_pipeline_id):
    # Duplicate the pipeline in pipelines table
    cur.execute('''
        INSERT INTO pipelines (pipeline_id, tag, owner, notes)
        SELECT ?, tag, owner, notes
        FROM pipelines
        WHERE pipeline_id = ?
    ''', (new_pipeline_id, source_pipeline_id))

    return new_pipeline_id

def duplicate_node_pipeline_relation(cur, source_pipeline_id, node_id, new_pipeline_id):
    # Given a source pipeline, and a node_id, insert a new row with new_pipeline_id
    # This is used when branching a pipeline. A new pipeline is created with a subgraph of the original one.
    # Then only the node_pipeline_relation is updated to releate a node to the new pipeline.
    cur.execute('''
        INSERT INTO node_pipeline_relation (last_update, user, node_id, pipeline_id)
        SELECT last_update, user, node_id, ?
        FROM node_pipeline_relation
        WHERE pipeline_id = ? AND node_id = ?
    ''', (new_pipeline_id, source_pipeline_id, node_id))

    return new_pipeline_id


def duplicate_pipeline(cur, source_pipeline_id, new_pipeline_id):
    # Duplicate the pipeline means creating a new pipeline with a new ID and duplicate all entry which referes to that

    # Duplicate pipelines table
    cur.execute('''
        INSERT INTO pipelines (pipeline_id, tag, owner, notes)
        SELECT ?, tag, owner, notes
        FROM pipelines
        WHERE pipeline_id = ?
    ''', (new_pipeline_id, source_pipeline_id))

    # Duplicate node_pipeline_relation table
    cur.execute('''
        INSERT INTO node_pipeline_relation (node_id, pipeline_id, last_update, user, node_tag)
        SELECT  node_id, ?, last_update, user, node_tag
        FROM node_pipeline_relation
        WHERE pipeline_id = ?
    ''', (new_pipeline_id, source_pipeline_id))

    return new_pipeline_id


def dupicate_node_in_pipeline(cur, source_node_id, new_node_id, pipeline_id):
    # Duplicate the node in a pipeline without copying relations

    # Duplicate nodes table
    cur.execute('''
        INSERT INTO nodes (node_id, status, notes)
        SELECT ?, status, notes
        FROM nodes
        WHERE node_id = ?
    ''', (new_node_id, source_node_id))

    # Duplicate node_pipeline_relation table
    cur.execute('''
        INSERT INTO node_pipeline_relation (last_update, user, node_id, pipeline_id)
        SELECT last_update, user, ?, pipeline_id
        FROM node_pipeline_relation
        WHERE node_id = ? AND pipeline_id = ?
    ''', (new_node_id, source_node_id, pipeline_id))

    return new_node_id

def copy_node_relations(cur, source_node_id, new_node_id):
    # Copy child relations
    cur.execute('''
        INSERT INTO node_relation (child_id, parent_id)
        SELECT ?, parent_id
        FROM node_relation
        WHERE child_id = ?
    ''', (new_node_id, source_node_id))

    # Copy parent relations
    cur.execute('''
        INSERT INTO node_relation (child_id, parent_id)
        SELECT child_id, ?
        FROM node_relation
        WHERE parent_id = ?
    ''', (new_node_id, source_node_id))

    return new_node_id

def remove_pipeline_from_pipeline(cur, pipeline_id):
    # Remove the pieline from pipeline tables.
    # This way the pipeline is still available in the database, but not visible in the UI,
    # and can be repristinated.
    cur.execute('DELETE FROM pipelines WHERE pipeline_id = ?', (pipeline_id,))

def remove_pipeline_from_everywhere(cur, pipeline_id):
    # Remove the pipeline from all tables
    cur.execute('DELETE FROM pipelines WHERE pipeline_id = ?', (pipeline_id,))
    cur.execute('DELETE FROM node_pipeline_relation WHERE pipeline_id = ?', (pipeline_id,))
    update_editable_status_for_all_nodes(cur)

def duplicate_node_in_pipeline_with_relations(cur, source_node_id, new_node_id, pipeline_id):
    # Duplicate the node in a pipeline with copying relations
    dupicate_node_in_pipeline(cur, source_node_id, new_node_id, pipeline_id)
    copy_node_relations(cur, source_node_id, new_node_id)
    return new_node_id

def get_pipelines_with_node(cur, node_id):
    cur.execute('SELECT pipeline_id FROM node_pipeline_relation WHERE node_id = ?', (node_id,))
    return [row[0] for row in cur.fetchall()]

def count_pipeline_with_node(cur, node_id):
    cur.execute('SELECT COUNT(*) FROM node_pipeline_relation WHERE node_id = ?', (node_id,))
    row = cur.fetchone()
    return row[0] if row else 0

def is_node_editable(cur, node_id):
    cur.execute('SELECT editable FROM nodes WHERE node_id = ?', (node_id,))
    row = cur.fetchone()
    return bool(row[0])

def update_editable_status_for_all_nodes(cur):
    # Get the list of all nodes
    cur.execute('SELECT node_id FROM nodes')
    nodes = [row[0] for row in cur.fetchall()]

    report = []

    for node_id in nodes:
        # Check the number of pipelines the node is associated with
        cur.execute('SELECT COUNT(*) FROM node_pipeline_relation WHERE node_id = ?', (node_id,))
        pipeline_count = cur.fetchone()[0]

        # Update editable status based on the pipeline count
        if pipeline_count <= 1:
            cur.execute('UPDATE nodes SET editable = TRUE WHERE node_id = ?', (node_id,))
        else:
            cur.execute('UPDATE nodes SET editable = FALSE WHERE node_id = ?', (node_id,))
            report.append(node_id)

    # Print a report of nodes that did not match the logic
    if report:
        print("Nodes with editable set to FALSE due to being in multiple pipelines:")
        for node_id in report:
            print(f" - Node ID: {node_id}")
    else:
        print("All nodes are editable.")

def get_pipeline_notes(cur, pipeline_id):
    cur.execute('SELECT notes FROM pipelines WHERE pipeline_id = ?', (pipeline_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_pipeline_owner(cur, pipeline_id):
    cur.execute('SELECT owner FROM pipelines WHERE pipeline_id = ?', (pipeline_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_node_notes(cur, node_id):
    cur.execute('SELECT notes FROM nodes WHERE node_id = ?', (node_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_rows_with_pipeline_id_in_pipeline_description(cur, pipeline_id):
    cur.execute('SELECT * FROM pipeline_description WHERE pipeline_id = ?', (pipeline_id,))
    return cur.fetchall()

def get_all_pipeline_ids(cur):
    cur.execute('SELECT pipeline_id FROM pipelines')
    return [row[0] for row in cur.fetchall()]

def sanitize_node_relation(cur, pipeline_id):
    """
    Inside a pipeline, nodes cannot be attached to nodes that do not belong to the same pipeline.
    This function removes any node relations that violate this rule.
    It checks all nodes in the pipeline and removes relations where the child node is not part of the pipeline.
    """
    # Get all nodes in the pipeline
    cur.execute('SELECT node_id FROM node_pipeline_relation WHERE pipeline_id = ?', (pipeline_id,))
    pipeline_nodes = {row[0] for row in cur.fetchall()}

    # Get all node relations
    cur.execute('SELECT id, child_id, parent_id FROM node_relation')
    relations = cur.fetchall()

    for node_id in pipeline_nodes:
        if is_node_editable(cur, node_id):
            # Get all relations where the node is a child
            cur.execute('SELECT id, parent_id FROM node_relation WHERE child_id = ?', (node_id,))
            child_relations = cur.fetchall()

            # Remove relations where the parent node is not in the pipeline
            for relation_id, parent_id in child_relations:
                if parent_id not in pipeline_nodes:
                    cur.execute('DELETE FROM node_relation WHERE id = ?', (relation_id,))


def remove_node_relation_with_editable_logic(cur, parent_id, child_id):
    """
    Can only remove relation between nodes if childen is editable
    """
    if is_node_editable(cur,child_id):
        # Remove the relation
        cur.execute('DELETE FROM node_relation WHERE parent_id = ? AND child_id = ?', (parent_id, child_id))
        return cur.rowcount
    else:
        raise ValueError(f"Cannot remove relation: Node {child_id} is not editable.")

def update_node_notes(cur, node_id, notes):
    cur.execute('UPDATE nodes SET notes = ? WHERE node_id = ?', (notes, node_id))
    return cur.rowcount
