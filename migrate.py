import os
import clickhouse_connect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host=os.getenv('CLICKHOUSE_HOST'),
    port=os.getenv('CLICKHOUSE_PORT'),
    username=os.getenv('CLICKHOUSE_USERNAME'),
    password=os.getenv('CLICKHOUSE_PASSWORD'),
    database=os.getenv('CLICKHOUSE_DATABASE')
)
# Directory containing migration files  -------------------- important for server end
# migration_base_path = os.getenv("CLICKHOUSE_BASE_DIR")
# os.chdir(migration_base_path)

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
        try:
            with open(os.path.join(migration_dir, migration), 'r') as f:
                sql = f.read()
                client.command(sql)  # Execute the migration SQL
                print(f"Applied migration {migration_id}")
                # Log the applied migration
                client.command("INSERT INTO migration_log (id) VALUES (%s)", (migration_id,))
        except Exception as e:
            print(f"Error applying migration {migration_id}: {e}")

print("All migrations applied.")

# Apply views from structured and unstructured directories
structured_views_dir = './views/structured'
unstructured_views_dir = './views/unstructured'

# Apply views in structured folder
structured_views = [f for f in os.listdir(structured_views_dir) if f.endswith('.sql')]
for view in structured_views:
    try:
        with open(os.path.join(structured_views_dir, view), 'r') as f:
            sql = f.read()
            client.command(sql)
            print(f"Applied structured view {view}")
    except Exception as e:
        print(f"Error applying structured view {view}: {e}")

# Apply views in unstructured folder
unstructured_views = [f for f in os.listdir(unstructured_views_dir) if f.endswith('.sql')]
for view in unstructured_views:
    try:
        with open(os.path.join(unstructured_views_dir, view), 'r') as f:
            sql = f.read()
            client.command(sql)
            print(f"Applied unstructured view {view}")
    except Exception as e:
        print(f"Error applying unstructured view {view}: {e}")