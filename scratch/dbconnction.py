path_to_db = r"C:\Users\franc\Documents\fusionpipe\bin\connection.db"


from fusionpipe.utils.db_utils import load_db, connect_to_db

conn = load_db(path_to_db)

conn = connect_to_db(path_to_db)