# Admin helper fuction to dump the database onto CSV files for quick inspection
import pandas as pd

def table_df(conn, table_name):
    """
    Dump a table from the database to a pandas DataFrame.
    :param conn: Database connection
    :param table_name: Name of the table to dump
    :return: pandas DataFrame containing the table data
    """
    query = f'SELECT * FROM {table_name}'
    return pd.read_sql_query(query, conn)


def dump_all_tables_to_csv(conn, output_dir):
  """
  Dump all tables from the database to separate CSV files.
  :param conn: Database connection
  :param output_dir: Directory where CSV files will be saved
  """
  cursor = conn.cursor()
  cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
  tables = [row[0] for row in cursor.fetchall()]
  for table in tables:
    df = table_df(conn, table)
    csv_path = f"{output_dir}/{table}.csv"
    df.to_csv(csv_path, index=False)


if __name__ == "__main__":
    import argparse
    from fusionpipe.utils.db_utils import connect_to_db

    parser = argparse.ArgumentParser(description="Dump all database tables to CSV files.")
    parser.add_argument('--output-dir', required=True, help='Directory to save CSV files')
    args = parser.parse_args()

    conn = connect_to_db()
    dump_all_tables_to_csv(conn, args.output_dir)
    print(f"All tables dumped to CSV files in directory: {args.output_dir}")