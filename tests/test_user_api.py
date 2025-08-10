import os
import pytest
from fp_user_utils import user_api
from fusionpipe.utils import db_utils
import pytest

def test_get_node_id(monkeypatch, tmp_path):
    # Create a .node_id file with the expected node ID
    node_id = 'n_12345678901234_5678'
    node_id_file = tmp_path / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(tmp_path)
    assert user_api.get_current_node_id() == node_id


def test_get_folder_path_node(pg_test_db, monkeypatch, tmp_path):
    cur = db_utils.init_db(pg_test_db)
    node_id = 'n_12345678901234_5678'
    folder_path = str(tmp_path)
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (node_id, folder_path))
    pg_test_db.commit()
    monkeypatch.setattr(user_api, 'connect_to_db', lambda db_url=None: pg_test_db)
    
    # Create .node_id file instead of relying on directory name
    node_id_file = tmp_path / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(tmp_path)
    
    assert user_api.get_current_node_folder_path() == folder_path
    cur.close()


def test_get_folder_path_code(pg_test_db, monkeypatch, tmp_path):
    node_id = 'n_12345678901234_5678'
    node_folder = tmp_path / node_id
    node_folder.mkdir()
    code_folder = node_folder / 'code'
    code_folder.mkdir()
    cur = db_utils.init_db(pg_test_db)
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (node_id, str(node_folder)))
    pg_test_db.commit()
    monkeypatch.setattr(user_api, 'connect_to_db', lambda db_url=None: pg_test_db)
    
    # Create .node_id file in the node folder
    node_id_file = node_folder / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(node_folder)
    
    assert user_api.get_current_node_folder_path_code() == str(code_folder)
    cur.close()


def test_get_folder_path_data(pg_test_db, monkeypatch, tmp_path):
    node_id = 'n_12345678901234_5678'
    node_folder = tmp_path / node_id
    node_folder.mkdir()
    data_folder = node_folder / 'data'
    data_folder.mkdir()
    cur = db_utils.init_db(pg_test_db)
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (node_id, str(node_folder)))
    pg_test_db.commit()
    monkeypatch.setattr(user_api, 'connect_to_db', lambda db_url=None: pg_test_db)
    
    # Create .node_id file in the node folder
    node_id_file = node_folder / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(node_folder)
    
    assert user_api.get_current_node_folder_path_data() == str(data_folder)
    cur.close()


def test_get_folder_path_reports(pg_test_db, monkeypatch, tmp_path):
    node_id = 'n_12345678901234_5678'
    node_folder = tmp_path / node_id
    node_folder.mkdir()
    reports_folder = node_folder / 'reports'
    reports_folder.mkdir()
    cur = db_utils.init_db(pg_test_db)
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (node_id, str(node_folder)))
    pg_test_db.commit()
    monkeypatch.setattr(user_api, 'connect_to_db', lambda db_url=None: pg_test_db)
    
    # Create .node_id file in the node folder
    node_id_file = node_folder / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(node_folder)
    
    assert user_api.get_current_node_folder_path_reports() == str(reports_folder)
    cur.close()


def test_get_info_parents(pg_test_db, monkeypatch, tmp_path):
    # Setup: create parent and child nodes and their relation
    parent_id = 'n_parent_1'
    parent_tag = 'parent_tag'
    parent_folder = str(tmp_path / 'parent_folder')
    os.makedirs(parent_folder, exist_ok=True)
    child_id = 'n_child_1'
    child_folder = str(tmp_path / 'child_folder')
    os.makedirs(child_folder, exist_ok=True)

    cur = db_utils.init_db(pg_test_db)
    cur.execute('INSERT INTO nodes (node_id, folder_path, node_tag) VALUES (%s, %s, %s)', (parent_id, parent_folder, parent_tag))
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (child_id, child_folder))
    cur.execute('INSERT INTO node_relation (parent_id, child_id, edge_id) VALUES (%s, %s, %s)', (parent_id, child_id, '01'))
    pg_test_db.commit()

    monkeypatch.setattr(user_api, 'connect_to_db', lambda db_url=None: pg_test_db)

    result = user_api.get_info_parents(child_id)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]['node_id'] == parent_id
    assert result[0]['node_tag'] == parent_tag
    assert result[0]['folder_path'] == parent_folder
    assert result[0]['edge_id'] == '01'

    cur.close()

def test_get_parent_info_from_edge_id_found(pg_test_db, monkeypatch, tmp_path):
    # Setup: create parent and child nodes and their relation
    parent_id = 'n_parent_2'
    parent_tag = 'parent_tag_2'
    parent_folder = str(tmp_path / 'parent_folder_2')
    os.makedirs(parent_folder, exist_ok=True)
    child_id = 'n_child_2'
    child_folder = str(tmp_path / 'child_folder_2')
    os.makedirs(child_folder, exist_ok=True)
    edge_id = 'edge_42'

    cur = db_utils.init_db(pg_test_db)
    cur.execute('INSERT INTO nodes (node_id, folder_path, node_tag) VALUES (%s, %s, %s)', (parent_id, parent_folder, parent_tag))
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (child_id, child_folder))
    cur.execute('INSERT INTO node_relation (parent_id, child_id, edge_id) VALUES (%s, %s, %s)', (parent_id, child_id, edge_id))
    pg_test_db.commit()

    monkeypatch.setattr(user_api, 'connect_to_db', lambda db_url=None: pg_test_db)

    # Create .node_id file in the child folder
    node_id_file = os.path.join(child_folder, '.node_id')
    with open(node_id_file, 'w') as f:
        f.write(child_id)
    monkeypatch.chdir(child_folder)

    result = user_api.get_parent_info_from_edge_id(edge_id)
    assert isinstance(result, dict)
    assert result['node_id'] == parent_id
    assert result['node_tag'] == parent_tag
    assert result['folder_path'] == parent_folder
    assert result['edge_id'] == edge_id

    cur.close()


def test_get_parent_info_from_edge_id_not_found(pg_test_db, monkeypatch, tmp_path):
    # Setup: create child node only, no parent relation
    child_id = 'n_child_3'
    child_folder = str(tmp_path / 'child_folder_3')
    os.makedirs(child_folder, exist_ok=True)

    cur = db_utils.init_db(pg_test_db)
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (child_id, child_folder))
    pg_test_db.commit()

    monkeypatch.setattr(user_api, 'connect_to_db', lambda db_url=None: pg_test_db)

    # Create .node_id file in the child folder
    node_id_file = os.path.join(child_folder, '.node_id')
    with open(node_id_file, 'w') as f:
        f.write(child_id)
    monkeypatch.chdir(child_folder)

    result = user_api.get_parent_info_from_edge_id('nonexistent_edge')
    assert result is None

    cur.close()


def test_get_parent_folder_path_from_edge_id_found(pg_test_db, monkeypatch, tmp_path):
    # Setup: create parent and child nodes and their relation
    parent_id = 'n_parent_4'
    parent_tag = 'parent_tag_4'
    parent_folder = str(tmp_path / 'parent_folder_4')
    os.makedirs(parent_folder, exist_ok=True)
    child_id = 'n_child_4'
    child_folder = str(tmp_path / 'child_folder_4')
    os.makedirs(child_folder, exist_ok=True)
    edge_id = 'edge_99'

    cur = db_utils.init_db(pg_test_db)
    cur.execute('INSERT INTO nodes (node_id, folder_path, node_tag) VALUES (%s, %s, %s)', (parent_id, parent_folder, parent_tag))
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (child_id, child_folder))
    cur.execute('INSERT INTO node_relation (parent_id, child_id, edge_id) VALUES (%s, %s, %s)', (parent_id, child_id, edge_id))
    pg_test_db.commit()

    monkeypatch.setattr(user_api, 'connect_to_db', lambda db_url=None: pg_test_db)

    # Create .node_id file in the child folder
    node_id_file = os.path.join(child_folder, '.node_id')
    with open(node_id_file, 'w') as f:
        f.write(child_id)
    monkeypatch.chdir(child_folder)

    result = user_api.get_parent_folder_path_from_edge_id(edge_id)
    assert result == parent_folder

    cur.close()


def test_get_parent_folder_path_from_edge_id_not_found(pg_test_db, monkeypatch, tmp_path):
    # Setup: create child node only, no parent relation
    child_id = 'n_child_5'
    child_folder = str(tmp_path / 'child_folder_5')
    os.makedirs(child_folder, exist_ok=True)

    cur = db_utils.init_db(pg_test_db)
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (child_id, child_folder))
    pg_test_db.commit()

    monkeypatch.setattr(user_api, 'connect_to_db', lambda db_url=None: pg_test_db)

    # Create .node_id file in the child folder
    node_id_file = os.path.join(child_folder, '.node_id')
    with open(node_id_file, 'w') as f:
        f.write(child_id)
    monkeypatch.chdir(child_folder)

    result = user_api.get_parent_folder_path_from_edge_id('missing_edge')
    assert result is None

    cur.close()
