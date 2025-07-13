import os
import pytest
from fusionpipe.user_utils.python_user_utils import node_api
from fusionpipe.utils import db_utils
import pytest

def test_get_all_parent_node_folder_paths(pg_test_db, monkeypatch, dag_dummy_1):
    from fusionpipe.utils.pip_utils import graph_to_db
    from conftest import DATABASE_URL_TEST

    # Initialise DB schema
    cur = db_utils.init_db(pg_test_db)
    # Add the dummy DAG to the database
    graph_to_db(dag_dummy_1, cur)
    pg_test_db.commit()
    # Patch connect_to_db to use our test connection
    monkeypatch.setattr(node_api, 'connect_to_db', lambda db_url=None: pg_test_db)
    # Pick a node with parents in the dummy DAG (e.g., 'B' or 'C')
    node_id = 'B'
    expected_parents = [k for k in dag_dummy_1.predecessors(node_id)]
    expected_paths = [dag_dummy_1.nodes[parent]['folder_path'] for parent in expected_parents]

    # Set DATABASE_URL_TEST only for this test using monkeypatch
    monkeypatch.setenv('DATABASE_URL_TEST', DATABASE_URL_TEST)

    result = node_api.get_all_parent_node_folder_paths(node_id)
    assert set(result) == set(expected_paths)
    cur.close()

def test_get_node_id(monkeypatch, tmp_path):
    # Create a .node_id file with the expected node ID
    node_id = 'n_12345678901234_5678'
    node_id_file = tmp_path / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(tmp_path)
    assert node_api.get_node_id() == node_id


def test_get_folder_path_node(pg_test_db, monkeypatch, tmp_path):
    cur = db_utils.init_db(pg_test_db)
    node_id = 'n_12345678901234_5678'
    folder_path = str(tmp_path)
    cur.execute('INSERT INTO nodes (node_id, folder_path) VALUES (%s, %s)', (node_id, folder_path))
    pg_test_db.commit()
    monkeypatch.setattr(node_api, 'connect_to_db', lambda db_url=None: pg_test_db)
    
    # Create .node_id file instead of relying on directory name
    node_id_file = tmp_path / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(tmp_path)
    
    assert node_api.get_folder_path_node() == folder_path
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
    monkeypatch.setattr(node_api, 'connect_to_db', lambda db_url=None: pg_test_db)
    
    # Create .node_id file in the node folder
    node_id_file = node_folder / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(node_folder)
    
    assert node_api.get_folder_path_code() == str(code_folder)
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
    monkeypatch.setattr(node_api, 'connect_to_db', lambda db_url=None: pg_test_db)
    
    # Create .node_id file in the node folder
    node_id_file = node_folder / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(node_folder)
    
    assert node_api.get_folder_path_data() == str(data_folder)
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
    monkeypatch.setattr(node_api, 'connect_to_db', lambda db_url=None: pg_test_db)
    
    # Create .node_id file in the node folder
    node_id_file = node_folder / '.node_id'
    node_id_file.write_text(node_id)
    monkeypatch.chdir(node_folder)
    
    assert node_api.get_folder_path_reports() == str(reports_folder)
    cur.close()


