import psycopg2

def migrate_database():
	"""
	Migrate data from fusionpipe_prod4 (v2_1_0) to fusionpipe_prod5 (v2_2_0)
	- Adds new tables node_group_relation and node_groups, leaves them empty.
	"""
	db_url = "dbname=fusionpipe_prod4 port=5432"
	conn = psycopg2.connect(db_url)
	cur = conn.cursor()
	try:
		# Create node_groups table
		cur.execute('''
			CREATE TABLE IF NOT EXISTS node_groups (
				group_id TEXT PRIMARY KEY,
				pipeline_id TEXT,
				tag TEXT DEFAULT NULL,
				collapsed BOOLEAN DEFAULT FALSE,
				pos_x DOUBLE PRECISION DEFAULT 0.0,
				pos_y DOUBLE PRECISION DEFAULT 0.0,
				width DOUBLE PRECISION DEFAULT 400.0,
				height DOUBLE PRECISION DEFAULT 300.0,
				FOREIGN KEY (pipeline_id) REFERENCES pipelines(pipeline_id)
			)
		''')

		# Create node_group_relation table
		cur.execute('''
			CREATE TABLE IF NOT EXISTS node_group_relation (
				node_id TEXT PRIMARY KEY,
				group_id TEXT,
				FOREIGN KEY (node_id) REFERENCES nodes(node_id),
				FOREIGN KEY (group_id) REFERENCES node_groups(group_id)
			)
		''')

		conn.commit()
		print("✅ Migration to v2_2_0 completed: tables created and left empty.")
	except Exception as e:
		print(f"❌ Migration failed: {e}")
		conn.rollback()
		raise
	finally:
		cur.close()
		conn.close()

if __name__ == "__main__":
	migrate_database()
