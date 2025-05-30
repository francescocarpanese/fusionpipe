import sqlite3


def load_db(db_path='pipeline.db'):
    conn = sqlite3.connect(db_path)
    return conn

def init_db(db_path='pipeline.db'):
    conn = sqlite3.connect(db_path)
    cur = init_graph_db(conn)
    conn.commit()
    return conn

def init_graph_db(conn):
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS pipelines (
            id TEXT PRIMARY KEY,
            tag TEXT DEFAULT NULL
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,
            status TEXT CHECK(status IN ('ready', 'running', 'completed', 'failed', 'staledata')) DEFAULT 'ready'
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS node_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT,
            node_id TEXT,
            pipeline_id TEXT,
            FOREIGN KEY (node_id) REFERENCES nodes(id),
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(id),
            UNIQUE (node_id, pipeline_id, tag)    
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            last_update timestamp DEFAULT CURRENT_TIMESTAMP,
            user TEXT,
            node_id TEXT,
            pipeline_id TEXT,
            FOREIGN KEY (node_id) REFERENCES nodes(id),
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(id),
            UNIQUE (node_id, pipeline_id)
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

    cur.execute('''
        CREATE TABLE IF NOT EXISTS pipeline_desciption (
            pipeline_id TEXT PRIMARY KEY,
            description TEXT,
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
        )
    ''')
    

    conn.commit()
    return cur


def add_pipeline(cur, pipeline_id, tag=None):
    cur.execute('INSERT INTO pipelines (id, tag) VALUES (?, ?)', (pipeline_id, tag))
    return cur.lastrowid

def add_node_to_nodes(cur, node_id):
    cur.execute('INSERT INTO nodes (node_id) VALUES (?)', (node_id,))
    return cur.lastrowid

def remove_node_from_pipeline(cur, node_id, pipeline_id):
    cur.execute('DELETE FROM entries WHERE node_id = ? AND pipeline_id = ?', (node_id, pipeline_id))
    return cur.rowcount

def remove_node_from_nodes(cur, node_id):
    cur.execute('DELETE FROM nodes WHERE node_id = ?', (node_id,))
    return cur.rowcount

def add_node_to_entries(cur, node_id, pipeline_id, user=None):
    cur.execute('INSERT INTO entries (node_id, pipeline_id, user) VALUES (?, ?, ?)', (node_id, pipeline_id, user))
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

def add_node_tag(cur, node_id, pipeline_id, tag):
    cur.execute('''
        INSERT INTO node_tags (node_id, pipeline_id, tag)
        VALUES (?, ?, ?)
        ON CONFLICT(node_id, pipeline_id, tag) DO UPDATE SET tag=excluded.tag
    ''', (node_id, pipeline_id, tag))
    return cur.lastrowid

def get_node_tag(cur, node_id, pipeline_id):
    cur.execute('SELECT tag FROM node_tags WHERE node_id = ? AND pipeline_id = ?', (node_id, pipeline_id))
    row = cur.fetchone()
    return row[0] if row else None

def add_pipeline_description(cur, pipeline_id, description):
    cur.execute('''
        INSERT INTO pipeline_desciption (pipeline_id, description)
        VALUES (?, ?)
        ON CONFLICT(pipeline_id) DO UPDATE SET description=excluded.description
    ''', (pipeline_id, description))
    return cur.lastrowid

def update_node_status(cur, node_id, status):
    cur.execute('UPDATE nodes SET status = ? WHERE node_id = ?', (status, node_id))
    return cur.rowcount

def get_node_status(cur, node_id):
    cur.execute('SELECT status FROM nodes WHERE node_id = ?', (node_id,))
    row = cur.fetchone()
    return row[0] if row else None

def get_pipeline_tag(cur, pipeline_id):
    cur.execute('SELECT tag FROM pipelines WHERE id = ?', (pipeline_id,))
    row = cur.fetchone()
    return row[0] if row else None

def check_pipeline_exists(cur, pipeline_id):
    cur.execute('SELECT 1 FROM pipelines WHERE id = ?', (pipeline_id,))
    return cur.fetchone() is not None

def get_all_nodes_from_pip_id(cur, pipeline_id):
    cur.execute('SELECT node_id FROM entries WHERE pipeline_id = ?', (pipeline_id,))
    return [row[0] for row in cur.fetchall()]

def get_nodes_without_pipeline(cur):
    cur.execute('''
        SELECT node_id FROM nodes
        WHERE node_id NOT IN (SELECT node_id FROM entries)
    ''')
    return [row[0] for row in cur.fetchall()]

def clear_database(cur):
    cur.execute('DELETE FROM pipelines')
    cur.execute('DELETE FROM nodes')
    cur.execute('DELETE FROM node_tags')
    cur.execute('DELETE FROM entries')
    cur.execute('DELETE FROM node_relation')
    cur.execute('DELETE FROM pipeline_desciption')
    return cur

def remove_node_from_tags(cur, node_id):
    cur.execute('DELETE FROM node_tags WHERE node_id = ?', (node_id,))
    return cur.rowcount

def remove_node_from_relations(cur, node_id):
    cur.execute('DELETE FROM node_relation WHERE child_id = ? OR parent_id = ?', (node_id, node_id))
    return cur.rowcount

def remove_node_from_entries(cur, node_id):
    cur.execute('DELETE FROM entries WHERE node_id = ?', (node_id,))
    return cur.rowcount

def remove_node_from_everywhere(cur, node_id):
    remove_node_from_entries(cur, node_id)
    remove_node_from_tags(cur, node_id)
    remove_node_from_relations(cur, node_id)
    remove_node_from_nodes(cur, node_id)
    return cur.rowcount