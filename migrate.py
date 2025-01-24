import os
from client_connect import Connection

# Connect to ClickHouse
client = Connection.client

# Directory containing migration files
migration_dir = './migrations'
migrations = sorted(os.listdir(migration_dir))

# Create a migration log table to keep track of applied migrations
client.command("""
CREATE TABLE IF NOT EXISTS migration_log (
    id String,
    applied_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
""")

# Fetch already applied migrations
applied_migrations = set(row[0] for row in client.query("SELECT id FROM migration_log").result_rows)

# Apply new migrations
for migration in migrations:
    migration_id = migration.split('_')[0]  # Extract ID from filename
    if migration_id not in applied_migrations:
        with open(os.path.join(migration_dir, migration), 'r') as f:
            sql = f.read()
            client.command(sql)
            client.command("INSERT INTO migration_log (id) VALUES (%s)", (migration_id,))
            print(f"Applied migration {migration_id}")

print("All migrations applied.")

# Apply views from structured and unstructured 

# Directories containing view definitions
structured_views_dir = './views/structured'
unstructured_views_dir = './views/unstructured'

# Apply views in structured folder
structured_views = [f for f in os.listdir(structured_views_dir) if f.endswith('.sql')]
for view in structured_views:
    with open(os.path.join(structured_views_dir, view), 'r') as f:
        sql = f.read()
        client.command(sql)
        print(f"Applied structured view {view}")

# Apply views in unstructured folder
unstructured_views = [f for f in os.listdir(unstructured_views_dir) if f.endswith('.sql')]
for view in unstructured_views:
    with open(os.path.join(unstructured_views_dir, view), 'r') as f:
        sql = f.read()
        client.command(sql)
        print(f"Applied unstructured view {view}")