import psycopg2
import os

table_names = [
    'node_relation',
    'node_pipeline_relation',
    'nodes',
    'pipeline_relation',
    'pipelines',
    'projects',
    'processes'
]

def connect_to_db(db_url=os.environ.get("DATABASE_URL")):
    # Example db_url: "dbname=<yourdb> user=<youruser> password=<yourpassword> host=localhost port=<port>"
    conn = psycopg2.connect(db_url)
    return conn

def create_db(db_path=os.environ.get("DATABASE_URL")):
    conn = connect_to_db(db_path)
    init_db(conn)
    return conn

def clear_all_tables(conn):
    """
    Clear all tables in the database in the correct order to avoid foreign key violations.
    :param conn: Database connection
    """
    cur = conn.cursor()
    # Clear all tables in the correct order
    for table in table_names:
        cur.execute(f"DELETE FROM {table}")
    conn.commit()
    cur.close()

def get_all_tables_names(conn):
    """
    Get the names of all tables in the database.
    :param conn: Database connection
    :return: List of table names
    """
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    return tables

def init_db(conn):
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            project_id TEXT PRIMARY KEY,
            tag TEXT DEFAULT NULL,
            notes TEXT DEFAULT NULL,
            owner TEXT DEFAULT NULL
        )
    ''')    

    cur.execute('''
        CREATE TABLE IF NOT EXISTS pipelines (
            pipeline_id TEXT PRIMARY KEY,
            tag TEXT UNIQUE DEFAULT NULL,
            owner TEXT DEFAULT NULL,
            notes TEXT DEFAULT NULL,
            project_id TEXT DEFAULT NULL,
            blocked BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (project_id) REFERENCES projects(project_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,
            status TEXT CHECK(status IN ('ready', 'running', 'completed', 'failed', 'staledata')) DEFAULT 'ready',
            referenced BOOLEAN DEFAULT FALSE,
            notes TEXT DEFAULT NULL,
            folder_path TEXT DEFAULT NULL,
            node_tag TEXT,
            blocked BOOLEAN DEFAULT FALSE
        )
    ''')

    # blocked=true:
    # - cannot be deleted from pipeline
    # - cannot run 
    # - writing permission are removed from owener and group
    # - can be referenced and duplicated

    # referenced=true:
    # - Present in multiple pipelines
    # - Can be deleted from pipeline
    # - Delete from pipeline, do not delete the folder, only remove relation.
    # - No writing permission for owner and group, independently of the blocked status.
    # - Cannot run
    # - A node that is not referenced can be referenced in another pipeline only if it is a head note of the pipeline

    cur.execute('''
        CREATE TABLE IF NOT EXISTS processes (
            process_id TEXT PRIMARY KEY,
            node_id TEXT,
            status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed')) DEFAULT 'pending',
            start_time TIMESTAMP DEFAULT NULL,
            end_time TIMESTAMP DEFAULT NULL
        )
    ''')    


    cur.execute('''
        CREATE TABLE IF NOT EXISTS node_relation (
            id SERIAL PRIMARY KEY,
            child_id TEXT,
            parent_id TEXT,
            edge_id TEXT,
            FOREIGN KEY (child_id) REFERENCES nodes(node_id),
            FOREIGN KEY (parent_id) REFERENCES nodes(node_id)
        )
    ''')


    cur.execute('''
        CREATE TABLE IF NOT EXISTS node_pipeline_relation (
            node_id TEXT,
            pipeline_id TEXT,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            position_x DOUBLE PRECISION DEFAULT 0.0,
            position_y DOUBLE PRECISION DEFAULT 0.0,
            FOREIGN KEY (node_id) REFERENCES nodes(node_id),
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(pipeline_id),
            PRIMARY KEY (node_id, pipeline_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS pipeline_relation (
            id SERIAL PRIMARY KEY,
            child_id TEXT,
            parent_id TEXT DEFAULT NULL,
            FOREIGN KEY (child_id) REFERENCES pipelines(pipeline_id),
            FOREIGN KEY (parent_id) REFERENCES pipelines(pipeline_id)
        )
    ''')

    conn.commit()
    return cur


def add_pipeline_to_pipelines(cur, pipeline_id, tag=None, owner=None, notes=None, project_id=None, blocked=False):
    if tag is None:
        tag = pipeline_id
    cur.execute('INSERT INTO pipelines (pipeline_id, tag, owner, notes, project_id, blocked) VALUES (%s, %s, %s, %s, %s, %s)', (pipeline_id, tag, owner, notes, project_id, blocked))
    return pipeline_id

def add_node_to_nodes(cur, node_id, status='ready', referenced=False, notes=None, folder_path=None, node_tag=None, blocked=False):
    if node_tag is None:
        node_tag = node_id
    cur.execute('INSERT INTO nodes (node_id, status, referenced, notes, folder_path, node_tag, blocked) VALUES (%s, %s, %s, %s, %s, %s, %s)', 
                (node_id, status, bool(referenced), notes, folder_path, node_tag, blocked))
    return node_id

def remove_node_from_nodes(cur, node_id):
    cur.execute('DELETE FROM nodes WHERE node_id = %s', (node_id,))
    return cur.rowcount

def get_node_tag(cur, node_id):
    cur.execute('SELECT node_tag FROM nodes WHERE node_id = %s', (node_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_node_blocked_status(cur, node_id):
    cur.execute('SELECT blocked FROM nodes WHERE node_id = %s', (node_id,))
    row = cur.fetchone()
    return bool(row[0]) if row else False

def update_node_blocked_status(cur, node_id, blocked):
    """
    Update the blocked status of a node.
    :param cur: Database cursor
    :param node_id: ID of the node to update
    :param blocked: New blocked status (True or False)
    :return: Number of rows affected
    """
    if not isinstance(blocked, bool):
        raise ValueError("blocked status must be a boolean value.")
    
    cur.execute('UPDATE nodes SET blocked = %s WHERE node_id = %s', (blocked, node_id))
    return cur.rowcount

def add_node_to_pipeline(cur, node_id, pipeline_id, position_x=0., position_y=0.):
    cur.execute('INSERT INTO node_pipeline_relation (node_id, pipeline_id, position_x, position_y) VALUES (%s, %s, %s, %s)', 
               (node_id, pipeline_id, position_x, position_y))
    
    # Check if the node is present in more than one pipeline
    cur.execute('SELECT COUNT(*) FROM node_pipeline_relation WHERE node_id = %s', (node_id,))
    count = cur.fetchone()[0]

    # If the node is present in more than one pipeline, set referenced to false
    if count > 1:
        update_referenced_status(cur, node_id, referenced=True)
    
    return node_id

def add_node_relation(cur, child_id, parent_id, edge_id):
    cur.execute('INSERT INTO node_relation (child_id, parent_id, edge_id) VALUES (%s, %s, %s)', (child_id, parent_id, edge_id))
    return

def get_node_parents(cur, node_id):
    cur.execute('SELECT parent_id FROM node_relation WHERE child_id = %s', (node_id,))
    return [row[0] for row in cur.fetchall()]

def update_node_status(cur, node_id, status):
    cur.execute('UPDATE nodes SET status = %s WHERE node_id = %s', (status, node_id))
    return cur.rowcount

def update_node_tag(cur, node_id, node_tag):
    cur.execute('UPDATE nodes SET node_tag = %s WHERE node_id = %s', (node_tag, node_id))
    return cur.rowcount

def get_node_status(cur, node_id):
    cur.execute('SELECT status FROM nodes WHERE node_id = %s', (node_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_pipeline_tag(cur, pipeline_id):
    cur.execute('SELECT tag FROM pipelines WHERE pipeline_id = %s', (pipeline_id,))
    row = cur.fetchone()
    return row[0] if row else None

def check_if_pipeline_exists(cur, pipeline_id):
    cur.execute('SELECT 1 FROM pipelines WHERE pipeline_id = %s', (pipeline_id,))
    return cur.fetchone() is not None

def get_all_nodes_from_pip_id(cur, pipeline_id):
    cur.execute('SELECT node_id FROM node_pipeline_relation WHERE pipeline_id = %s', (pipeline_id,))
    return [row[0] for row in cur.fetchall()]

def get_all_pipelines_from_project_id(cur, project_id):
    cur.execute('SELECT pipeline_id FROM pipelines WHERE project_id = %s', (project_id,))
    return [row[0] for row in cur.fetchall()]

def get_all_nodes_from_nodes(cur):
    cur.execute('SELECT node_id FROM nodes')
    return [row[0] for row in cur.fetchall()]

def check_if_node_exists(cur, node_id):
    cur.execute('SELECT 1 FROM nodes WHERE node_id = %s', (node_id,))
    return cur.fetchone() is not None

def check_if_project_exists(cur, project_id):
    cur.execute('SELECT 1 FROM projects WHERE project_id = %s', (project_id,))
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
    cur.execute('DELETE FROM node_relation WHERE child_id = %s OR parent_id = %s', (node_id, node_id))
    return cur.rowcount

def remove_node_from_node_pipeline_relation(cur, node_id):
    cur.execute('DELETE FROM node_pipeline_relation WHERE node_id = %s', (node_id,))
    return cur.rowcount

def remove_node_from_everywhere(cur, node_id):
    remove_node_from_node_pipeline_relation(cur, node_id)
    remove_node_from_relations(cur, node_id)
    remove_node_from_nodes(cur, node_id)
    return cur.rowcount

def get_rows_with_node_id_in_entries(cur, node_id):
    cur.execute('SELECT * FROM node_pipeline_relation WHERE node_id = %s', (node_id,))
    return cur.fetchall()

def get_rows_node_id_in_nodes(cur, node_id):
    cur.execute('SELECT * FROM nodes WHERE node_id = %s', (node_id,))
    return cur.fetchall()

def get_rows_with_node_id_relations(cur, node_id):
    cur.execute('SELECT * FROM node_relation WHERE child_id = %s OR parent_id = %s', (node_id, node_id))
    return cur.fetchall()

def get_rows_with_pipeline_id_in_entries(cur, pipeline_id):
    cur.execute('SELECT * FROM node_pipeline_relation WHERE pipeline_id = %s', (pipeline_id,))
    return cur.fetchall()

def get_rows_with_pipeline_id_in_pipelines(cur, pipeline_id):
    cur.execute('SELECT * FROM pipelines WHERE pipeline_id = %s', (pipeline_id,))
    return cur.fetchall()

def remove_node_from_pipeline(cur, node_id, pipeline_id):
    cur.execute('DELETE FROM node_pipeline_relation WHERE node_id = %s AND pipeline_id = %s', (node_id,pipeline_id))
    # Check if the node is still referenced in any other pipeline
    check_and_update_node_referenced_status(cur, node_id)
    sanitize_node_relation(cur, pipeline_id)
    return cur.rowcount    

def duplicate_pipeline_in_pipelines(cur, source_pipeline_id, new_pipeline_id):
    # Duplicate the pipeline in pipelines table
    cur.execute('''
        INSERT INTO pipelines (pipeline_id, tag, owner, notes)
        SELECT %s, tag, owner, notes
        FROM pipelines
        WHERE pipeline_id = %s
    ''', (new_pipeline_id, source_pipeline_id))

    return new_pipeline_id

def duplicate_node_pipeline_relation(cur, source_pipeline_id, node_ids, new_pipeline_id):
    # Given a source pipeline, and a node_id list, insert each node in a target pipeline
    # This is used when branching a pipeline. A new pipeline is created with a subgraph of the original one.
    # Only node_pipeline_relation is updated to releate a node to the new pipeline.
    if isinstance(node_ids, str):
        node_ids = [node_ids]

    for node_id in node_ids:
        cur.execute('''
            INSERT INTO node_pipeline_relation (last_update, node_id, pipeline_id, position_x, position_y)
            SELECT last_update, node_id, %s, position_x, position_y
            FROM node_pipeline_relation
            WHERE pipeline_id = %s AND node_id = %s
        ''', (new_pipeline_id, source_pipeline_id, node_id))

    # As node are now duplicated they cannot be edited anymore.
    for node_id in node_ids:
        update_referenced_status(cur, node_id, True)
    return new_pipeline_id


def duplicate_pipeline(cur, source_pipeline_id, new_pipeline_id):
    # Duplicate the pipeline means creating a new pipeline with a new ID and duplicate all entry which referes to that

    # Duplicate pipelines table
    cur.execute('''
        INSERT INTO pipelines (pipeline_id, tag, owner, notes)
        SELECT %s, %s, owner, notes
        FROM pipelines
        WHERE pipeline_id = %s
    ''', (new_pipeline_id, new_pipeline_id, source_pipeline_id))

    # Duplicate node_pipeline_relation table with positions
    cur.execute('''
        INSERT INTO node_pipeline_relation (node_id, pipeline_id, last_update, position_x, position_y)
        SELECT  node_id, %s, last_update, position_x, position_y
        FROM node_pipeline_relation
        WHERE pipeline_id = %s
    ''', (new_pipeline_id, source_pipeline_id))

    return new_pipeline_id


def dupicate_node_in_pipeline(cur, source_node_id, new_node_id, source_pipeline_id, target_pipeline_id):
    """
    Add the new node to the nodes and duplicate into the pipeline 
    """

    # Duplicate nodes table
    cur.execute('''
        INSERT INTO nodes (node_id, status, notes)
        SELECT %s, status, notes
        FROM nodes
        WHERE node_id = %s
    ''', (new_node_id, source_node_id))

    # Duplicate node_pipeline_relation table
    cur.execute('''
        INSERT INTO node_pipeline_relation (last_update, node_id, pipeline_id, position_x, position_y)
        SELECT last_update, %s, %s, position_x, position_y
        FROM node_pipeline_relation
        WHERE node_id = %s AND pipeline_id = %s
    ''', (new_node_id, target_pipeline_id, source_node_id, source_pipeline_id))    


    return new_node_id

def copy_node_relations(cur, source_node_id, new_node_id, childrens = False, parents = False):

    if parents:
        # Copy child relations
        cur.execute('''
            INSERT INTO node_relation (child_id, parent_id, edge_id)
            SELECT %s, parent_id, edge_id
            FROM node_relation
            WHERE child_id = %s
        ''', (new_node_id, source_node_id))

    if childrens:
        # Copy parent relations
        cur.execute('''
            INSERT INTO node_relation (child_id, parent_id, edge_id)
            SELECT child_id, %s, edge_id
            FROM node_relation
            WHERE parent_id = %s
        ''', (new_node_id, source_node_id))

    return new_node_id

def remove_pipeline_from_pipeline(cur, pipeline_id):
    # Remove the pieline from pipeline tables.
    # This way the pipeline is still available in the database, but not visible in the UI,
    # and can be repristinated.
    cur.execute('DELETE FROM pipelines WHERE pipeline_id = %s', (pipeline_id,))

def remove_pipeline_from_everywhere(cur, pipeline_id):
    if get_pipeline_blocked_status(cur, pipeline_id=pipeline_id):
        raise ValueError(f"Pipeline {pipeline_id} is blocked. Cannot remove pipeline.")
    # Remove the pipeline from all tables
    cur.execute('DELETE FROM node_pipeline_relation WHERE pipeline_id = %s', (pipeline_id,))
    cur.execute('DELETE FROM pipeline_relation WHERE child_id = %s OR parent_id = %s', (pipeline_id, pipeline_id))
    cur.execute('DELETE FROM pipelines WHERE pipeline_id = %s', (pipeline_id,))
    update_referenced_status_for_all_nodes(cur)

def duplicate_node_in_pipeline_with_relations(cur, source_node_id, new_node_id, source_pipeline_id, target_pipeline_id, parents=False, childrens=False):
    # Duplicate the node in a pipeline with copying relations
    dupicate_node_in_pipeline(cur, source_node_id, new_node_id, source_pipeline_id, target_pipeline_id)
    # Only parents are copied otherwise childrens will have a different input signature
    copy_node_relations(cur, source_node_id, new_node_id, parents=parents, childrens=childrens)
    return new_node_id

def get_pipelines_with_node(cur, node_id):
    cur.execute('SELECT pipeline_id FROM node_pipeline_relation WHERE node_id = %s', (node_id,))
    return [row[0] for row in cur.fetchall()]

def count_pipeline_with_node(cur, node_id):
    cur.execute('SELECT COUNT(*) FROM node_pipeline_relation WHERE node_id = %s', (node_id,))
    row = cur.fetchone()
    return row[0] if row else 0

def get_node_referenced_status(cur, node_id):
    cur.execute('SELECT referenced FROM nodes WHERE node_id = %s', (node_id,))
    row = cur.fetchone()
    return bool(row[0])

def check_and_update_node_referenced_status(cur, node_id):
    # Check the number of pipelines the node is associated with
    cur.execute('SELECT COUNT(*) FROM node_pipeline_relation WHERE node_id = %s', (node_id,))
    pipeline_count = cur.fetchone()[0]

    # Update referenced status based on the pipeline count
    if pipeline_count <= 1:
        cur.execute('UPDATE nodes SET referenced = FALSE WHERE node_id = %s', (node_id,))
    else:
        cur.execute('UPDATE nodes SET referenced = TRUE WHERE node_id = %s', (node_id,))    

def update_referenced_status_for_all_nodes(cur):
    # Get the list of all nodes
    cur.execute('SELECT node_id FROM nodes')
    nodes = [row[0] for row in cur.fetchall()]

    for node_id in nodes:
        # Update the referenced status for each node
        check_and_update_node_referenced_status(cur, node_id)

def get_pipeline_notes(cur, pipeline_id):
    cur.execute('SELECT notes FROM pipelines WHERE pipeline_id = %s', (pipeline_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_pipeline_owner(cur, pipeline_id):
    cur.execute('SELECT owner FROM pipelines WHERE pipeline_id = %s', (pipeline_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_node_notes(cur, node_id):
    cur.execute('SELECT notes FROM nodes WHERE node_id = %s', (node_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_rows_with_pipeline_id_in_pipeline_description(cur, pipeline_id):
    cur.execute('SELECT * FROM pipeline_description WHERE pipeline_id = %s', (pipeline_id,))
    return cur.fetchall()

def get_all_pipeline_ids(cur):
    cur.execute('SELECT pipeline_id FROM pipelines')
    return [row[0] for row in cur.fetchall()]

def get_all_pipeline_tags(cur):
    cur.execute('SELECT tag FROM pipelines')
    return [row[0] for row in cur.fetchall()]

def get_all_pipeline_ids_tags_dict(cur):
    cur.execute('SELECT pipeline_id, tag FROM pipelines ORDER BY pipeline_id')
    return {row[0]: row[1] for row in cur.fetchall()}

def sanitize_node_relation(cur, pipeline_id):
    """
    Inside a pipeline, nodes cannot be attached to nodes that do not belong to the same pipeline.
    This function removes any node relations that violate this rule.
    It checks all nodes in the pipeline and removes relations where the child node is not part of the pipeline.
    """
    # Get all nodes in the pipeline
    cur.execute('SELECT node_id FROM node_pipeline_relation WHERE pipeline_id = %s', (pipeline_id,))
    pipeline_nodes = {row[0] for row in cur.fetchall()}

    # Get all node relations
    cur.execute('SELECT id, child_id, parent_id FROM node_relation')
    relations = cur.fetchall()

    for node_id in pipeline_nodes:
        if not get_node_referenced_status(cur, node_id):
            # Get all relations where the node is a child
            cur.execute('SELECT id, parent_id FROM node_relation WHERE child_id = %s', (node_id,))
            child_relations = cur.fetchall()

            # Remove relations where the parent node is not in the pipeline
            for relation_id, parent_id in child_relations:
                if parent_id not in pipeline_nodes:
                    cur.execute('DELETE FROM node_relation WHERE id = %s', (relation_id,))


def remove_node_relation_with_referenced_logic(cur, parent_id, child_id):
    """
    Can only remove relation between nodes if child is not referenced
    """
    if not get_node_referenced_status(cur,child_id):
        # Remove the relation
        cur.execute('DELETE FROM node_relation WHERE parent_id = %s AND child_id = %s', (parent_id, child_id))
        return cur.rowcount
    else:
        raise ValueError(f"Cannot remove relation: Node {child_id} is referenced, and present in multiple pipeline.")

def update_node_notes(cur, node_id, notes):
    cur.execute('UPDATE nodes SET notes = %s WHERE node_id = %s', (notes, node_id))
    return cur.rowcount

def update_pipeline_tag(cur, pipeline_id, tag):
    cur.execute('UPDATE pipelines SET tag = %s WHERE pipeline_id = %s', (tag, pipeline_id))
    return cur.rowcount

def update_pipeline_notes(cur, pipeline_id, notes):
    cur.execute('UPDATE pipelines SET notes = %s WHERE pipeline_id = %s', (notes, pipeline_id))
    return cur.rowcount

def get_node_position(cur, node_id, pipeline_id):
    cur.execute('SELECT position_x, position_y FROM node_pipeline_relation WHERE node_id = %s AND pipeline_id = %s', (node_id, pipeline_id))
    result = cur.fetchone()
    if result and result[0] is not None and result[1] is not None:
        return [result[0], result[1]]
    return None

def get_node_folder_path(cur, node_id):
    cur.execute('SELECT folder_path FROM nodes WHERE node_id = %s', (node_id,))
    row = cur.fetchone()
    return row[0] if row else None

def update_node_position(cur, node_id, pipeline_id, position_x, position_y):
    cur.execute('UPDATE node_pipeline_relation SET position_x = %s, position_y = %s WHERE node_id = %s AND pipeline_id = %s', 
               (position_x, position_y, node_id, pipeline_id))
    return cur.rowcount

def update_folder_path_node(cur, node_id, folder_path):
    cur.execute('UPDATE nodes SET folder_path = %s WHERE node_id = %s', (folder_path, node_id))
    return cur.rowcount

def update_referenced_status(cur, node_id, referenced):
    """
    Update the referenced status of a node.
    :param cur: Database cursor
    :param node_id: ID of the node to update
    :param referenced: New referenced status (True or False)
    :return: Number of rows affected
    """
    if not isinstance(referenced, bool):
        raise ValueError("referenced status must be a boolean value.")
    
    cur.execute('UPDATE nodes SET referenced = %s WHERE node_id = %s', (referenced, node_id))
    return cur.rowcount

def add_process(cur, process_id, node_id, status='pending', start_time=None, end_time=None):
    """
    Add a new process to the processes table.
    :param cur: Database cursor
    :param process_id: Unique ID for the process
    :param node_id: ID of the node associated with the process
    :param status: Status of the process (default is 'pending')
    :param start_time: Start time of the process (default is None)
    :param end_time: End time of the process (default is None)
    :return: Last row ID inserted
    """
    cur.execute('INSERT INTO processes (process_id, node_id, status, start_time, end_time) VALUES (%s, %s, %s, %s, %s)', 
                (process_id, node_id, status, start_time, end_time))
    return cur.lastrowid

def remove_process(cur, process_id):
    """
    Remove a process from the processes table.
    :param cur: Database cursor
    :param process_id: ID of the process to remove
    :return: Number of rows affected
    """
    cur.execute('DELETE FROM processes WHERE process_id = %s', (str(process_id),))
    return cur.rowcount

def get_processes_by_node(cur, node_id):
    """
    Get all processes associated with a specific node.
    :param cur: Database cursor
    :param node_id: ID of the node to query
    :return: List of processes associated with the node
    """
    # Use cursor that returns rows as dictionaries if possible
    cur.execute('SELECT * FROM processes WHERE node_id = %s', (node_id,))
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]

def update_process_status(cur, process_id, status):
    """
    Update the status of a process.
    :param cur: Database cursor
    :param process_id: ID of the process to update
    :param status: New status for the process
    :return: Number of rows affected
    """
    cur.execute('UPDATE processes SET status = %s WHERE process_id = %s', (status, str(process_id)))
    return cur.rowcount

def get_process_ids_by_node(cur, node_id):
    """
    Get all process IDs associated with a specific node.
    :param cur: Database cursor
    :param node_id: ID of the node to query
    :return: List of process IDs associated with the node
    """
    cur.execute('SELECT process_id FROM processes WHERE node_id = %s', (node_id,))
    return [row[0] for row in cur.fetchall()]  # Return only the process IDs

def add_project(cur, project_id, tag=None, notes=None, owner=None):
    if tag is None:
        tag = project_id
    cur.execute('INSERT INTO projects (project_id, tag, notes, owner) VALUES (%s, %s, %s, %s)', (project_id, tag, notes, owner))
    return project_id

def add_project_to_pipeline(cur, project_id, pipeline_id):
    """
    Add a project to a pipeline.
    :param cur: Database cursor
    :param project_id: ID of the project to add
    :param pipeline_id: ID of the pipeline to associate with the project
    :return: Number of rows affected
    """
    cur.execute('UPDATE pipelines SET project_id = %s WHERE pipeline_id = %s', (project_id, pipeline_id))
    return cur.rowcount

def remove_project_from_pipeline(cur, project_id, pipeline_id):
    """
    Remove a project from a pipeline.
    :param cur: Database cursor
    :param project_id: ID of the project to remove
    :param pipeline_id: ID of the pipeline to disassociate from the project
    :return: Number of rows affected
    """
    cur.execute('UPDATE pipelines SET project_id = NULL WHERE pipeline_id = %s', (pipeline_id,))
    return cur.rowcount

def remove_project_from_all_pipelines(cur, project_id):
    """
    Remove a project from all pipelines.
    :param cur: Database cursor
    :param project_id: ID of the project to remove from all pipelines
    :return: Number of rows affected
    """
    cur.execute('UPDATE node_pipeline_relation SET project_id = NULL WHERE project_id = %s', (project_id,))
    return cur.rowcount

def get_all_projects(cur):
    """
    Get all projects from the projects table.
    :param cur: Database cursor
    :return: List of all projects as dictionaries
    """
    cur.execute('SELECT * FROM projects')
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]  # Convert rows to dictionaries for easier access

def check_project_exists(cur, project_id):
    """
    Check if a project exists in the projects table.
    :param cur: Database cursor
    :param project_id: ID of the project to check
    :return: True if the project exists, False otherwise
    """
    cur.execute('SELECT 1 FROM projects WHERE project_id = %s', (project_id,))
    return cur.fetchone() is not None

def get_project_by_id(cur, project_id):
    """
    Get a specific project by its ID.
    :param cur: Database cursor
    :param project_id: ID of the project to query
    :return: Project as a dictionary or None if not found
    """
    cur.execute('SELECT * FROM projects WHERE project_id = %s', (project_id,))
    row = cur.fetchone()
    if row:
        colnames = [desc[0] for desc in cur.description]
        return dict(zip(colnames, row))
    else:
        return None  # Return None if not found

def remove_project(cur, project_id):
    """
    Remove a project from the projects table.
    :param cur: Database cursor
    :param project_id: ID of the project to remove
    :return: Number of rows affected
    """
    cur.execute('DELETE FROM projects WHERE project_id = %s', (project_id,))
    return cur.rowcount


def update_project_tag(cur, project_id, tag):
    """
    Update the tag of a project.
    :param cur: Database cursor
    :param project_id: ID of the project to update
    :param tag: New tag for the project
    :return: Number of rows affected
    """
    cur.execute('UPDATE projects SET tag = %s WHERE project_id = %s', (tag, project_id))
    return cur.rowcount

def update_project_notes(cur, project_id, notes):
    """
    Update the notes of a project.
    :param cur: Database cursor
    :param project_id: ID of the project to update
    :param notes: New notes for the project
    :return: Number of rows affected
    """
    cur.execute('UPDATE projects SET notes = %s WHERE project_id = %s', (notes, project_id))
    return cur.rowcount

def get_pipeline_ids_by_project(cur, project_id):
    """
    Get all pipeline IDs associated with a specific project.
    :param cur: Database cursor
    :param project_id: ID of the project to query
    :return: List of pipeline IDs associated with the project
    """
    cur.execute('SELECT DISTINCT pipeline_id FROM pipelines WHERE project_id = %s', (project_id,))
    return [row[0] for row in cur.fetchall()]  # Return only the pipeline IDs

def get_all_project_ids_tags_dict(cur):
    cur.execute('SELECT project_id, tag FROM projects ORDER BY project_id')
    return {row[0]: row[1] for row in cur.fetchall()}

def remove_project_from_everywhere(cur, project_id):
    """
    Remove a project from all tables.
    :param cur: Database cursor
    :param project_id: ID of the project to remove
    :return: Number of rows affected
    """
    cur.execute('UPDATE pipelines SET project_id = NULL WHERE project_id = %s', (project_id,))    
    cur.execute('DELETE FROM projects WHERE project_id = %s', (project_id,))
    return cur.rowcount

def get_project_id_by_pipeline(cur, pipeline_id):
    """
    Get the project ID associated with a specific pipeline.
    :param cur: Database cursor
    :param pipeline_id: ID of the pipeline to query
    :return: Project ID associated with the pipeline, or empty string if not found
    """
    cur.execute('SELECT project_id FROM pipelines WHERE pipeline_id = %s', (pipeline_id,))
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else ""

def add_pipeline_relation(cur, child_id, parent_id=None):
    """
    Add a relation between pipelines.
    :param cur: Database cursor
    :param child_id: ID of the child pipeline
    :param parent_id: ID of the parent pipeline (optional)
    :return: Number of rows affected
    """
    cur.execute('INSERT INTO pipeline_relation (child_id, parent_id) VALUES (%s, %s)', (child_id, parent_id))
    return cur.rowcount

def remove_pipeline_relation(cur, child_id, parent_id=None):
    """
    Remove a relation between pipelines.
    :param cur: Database cursor
    :param child_id: ID of the child pipeline
    :param parent_id: ID of the parent pipeline (optional)
    :return: Number of rows affected
    """
    if parent_id:
        cur.execute('DELETE FROM pipeline_relation WHERE child_id = %s AND parent_id = %s', (child_id, parent_id))
    else:
        cur.execute('DELETE FROM pipeline_relation WHERE child_id = %s', (child_id,))
    return cur.rowcount

def remove_all_pipeline_relation_of_pipeline_id(cur, pipeline_id):
    """
    Remove all relations of a pipeline by its ID.
    :param cur: Database cursor
    :param pipeline_id: ID of the pipeline to remove relations for
    :return: Number of rows affected
    """
    cur.execute('DELETE FROM pipeline_relation WHERE child_id = %s OR parent_id = %s', (pipeline_id, pipeline_id))
    return cur.rowcount

def get_pipeline_parents(cur, pipeline_id):
    """
    Get all parent pipelines of a given pipeline.
    :param cur: Database cursor
    :param pipeline_id: ID of the pipeline to query
    :return: List of parent pipeline IDs
    """
    cur.execute('SELECT parent_id FROM pipeline_relation WHERE child_id = %s', (pipeline_id,))
    return [row[0] for row in cur.fetchall()]

def get_project_notes(cur, project_id):
    """
    Get the notes of a specific project.
    :param cur: Database cursor
    :param project_id: ID of the project to query
    :return: Notes of the project, or None if not found
    """
    cur.execute('SELECT notes FROM projects WHERE project_id = %s', (project_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_project_owner(cur, project_id):
    """
    Get the owner of a specific project.
    :param cur: Database cursor
    :param project_id: ID of the project to query
    :return: Owner of the project, or None if not found
    """
    cur.execute('SELECT owner FROM projects WHERE project_id = %s', (project_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_project_tag(cur, project_id):
    """
    Get the tag of a specific project.
    :param cur: Database cursor
    :param project_id: ID of the project to query
    :return: Tag of the project, or None if not found
    """
    cur.execute('SELECT tag FROM projects WHERE project_id = %s', (project_id,))
    row = cur.fetchone()
    return row[0] if row else None

def is_pipeline_blocked(cur, pipeline_id):
    """
    Set the blocked status of a pipeline based on the blocked status of its nodes.
    A pipeline is blocked if all nodes in the pipeline are blocked.
    :param cur: Database cursor
    :param pipeline_id: ID of the pipeline to check
    :return: True if the pipeline is blocked, False otherwise
    """
    # Count total nodes in the pipeline
    cur.execute('SELECT COUNT(*) FROM node_pipeline_relation WHERE pipeline_id = %s', (pipeline_id,))
    total_nodes = cur.fetchone()[0]

    if total_nodes == 0:
        blocked = False
    else:
        # Count blocked nodes in the pipeline
        cur.execute('''
            SELECT COUNT(*)
            FROM node_pipeline_relation npr
            JOIN nodes n ON npr.node_id = n.node_id
            WHERE npr.pipeline_id = %s AND n.status = 'blocked'
        ''', (pipeline_id,))
        blocked_nodes = cur.fetchone()[0]
        blocked = (blocked_nodes == total_nodes)

    # Update the pipeline's blocked status
    cur.execute('UPDATE pipelines SET blocked = %s WHERE pipeline_id = %s', (blocked, pipeline_id))
    return blocked


def get_pipeline_blocked_status(cur, pipeline_id):
    """
    Get the blocked status of a specific pipeline.
    :param cur: Database cursor
    :param pipeline_id: ID of the pipeline to query
    :return: blocked status of the pipeline (True or False)
    """
    cur.execute('SELECT blocked FROM pipelines WHERE pipeline_id = %s', (pipeline_id,))
    row = cur.fetchone()
    return bool(row[0]) if row else None  # Return True or False based on the referenced status


def update_pipeline_blocked_status(cur, pipeline_id, blocked):
    """
    Update the blocked status of a specific pipeline.
    :param cur: Database cursor
    :param pipeline_id: ID of the pipeline to update
    :param blocked: New blocked status (True or False)
    :return: Number of rows affected
    """
    if not isinstance(blocked, bool):
        raise ValueError("referenced status must be a boolean value.")
    
    cur.execute('UPDATE pipelines SET blocked = %s WHERE pipeline_id = %s', (blocked, pipeline_id))
    return cur.rowcount

def check_node_relation_exists(cur, child_id, parent_id):
    """
    Check if a relation between nodes already exists in the node_relation table.
    :param cur: Database cursor
    :param child_id: ID of the child node
    :param parent_id: ID of the parent node
    :return: True if the relation exists, False otherwise
    """
    cur.execute('SELECT 1 FROM node_relation WHERE child_id = %s AND parent_id = %s', (child_id, parent_id))
    return cur.fetchone() is not None

def get_all_node_ids(cur):
    """
    Get all node IDs from the nodes table.
    :param cur: Database cursor
    :return: List of all node IDs
    """
    cur.execute('SELECT node_id FROM nodes')
    return [row[0] for row in cur.fetchall()]

def get_node_relation_edge_id(cur, child_id, parent_id):
    """
    Get the edge ID of a specific node relation.
    :param cur: Database cursor
    :param child_id: ID of the child node
    :param parent_id: ID of the parent node
    :return: Edge ID of the relation, or None if not found
    """
    cur.execute('SELECT edge_id FROM node_relation WHERE child_id = %s AND parent_id = %s', (child_id, parent_id))
    row = cur.fetchone()
    return row[0] if row else None

def get_edge_id_of_all_node_parents(cur, child_id):
    """
    Get the edge IDs of all parents of a specific child node.
    :param cur: Database cursor
    :param child_id: ID of the child node
    :return: List of edge IDs of all parent nodes
    """
    cur.execute('SELECT edge_id FROM node_relation WHERE child_id = %s', (child_id,))
    rows = cur.fetchall()
    return [row[0] for row in rows]

