# Admin utility to clear or recreate all database tables.
# This is useful during migration processes to reset a database.
# This operation is destructive and requires confirmation!
import argparse
from fusionpipe.utils.db_utils import connect_to_db, init_db, clear_all_tables, get_all_tables_names, drop_all_tables

def main():
    parser = argparse.ArgumentParser(description="Database table management utility.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--clear', action='store_true', help='Clear all tables (delete all data, keep schema)')
    group.add_argument('--recreate', action='store_true', help='Drop and recreate all tables (delete everything)')
    args = parser.parse_args()

    conn = connect_to_db()
    tables = get_all_tables_names(conn)
    print(f"Connected to database: {conn.info.dbname}")
    print(f"Current tables: {tables}")

    action = "clear ALL data from" if args.clear else "DROP and RECREATE all"
    confirm = input(f"Type the database name '{conn.info.dbname}' to confirm you want to {action} all tables: ")
    if confirm != conn.info.dbname:
        print("Database name does not match. Aborting operation.")
        return

    if args.clear:
        print("Clearing all tables...")
        clear_all_tables(conn)
        print("All tables cleared.")
    elif args.recreate:
        print("Dropping all tables...")
        drop_all_tables(conn)
        print("Re-initializing tables...")
        init_db(conn)
        print("All tables recreated.")

if __name__ == "__main__":
    main()

