from fusionpipe.utils.db_utils import create_db, connect_to_db, init_db, clear_all_tables, get_all_tables_names

# Assuming that a postgres database is already running and accessible.
# Set the enviroment variable DATABASE_URL="dbname=<dbname> user=<user> password=<password> host=localhost port=<dbport>"
# Make sure that you are putting the right database name to prevent deleting the database from production.

# Add a simple pipeline
conn = connect_to_db()
cur = conn.cursor()


init_db(conn)
tables = get_all_tables_names(conn)

print(f"Connected to database: {conn.info.dbname}")

if not tables:
    print("No tables found. Initializing the database...")
    init_db(conn)
else:
    print(f"Database contains the following tables: {tables}")
    db_name = input("Enter the name of the database to confirm clearing all tables: ")
    if db_name == conn.info.dbname:
        print("Clearing all tables...")
        clear_all_tables(conn)
    else:
        print("Database name does not match. Aborting operation.")