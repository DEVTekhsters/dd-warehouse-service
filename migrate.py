import os
import clickhouse_connect

# Connect to ClickHouse
client = clickhouse_connect.get_client(host='13.202.114.233', username='default', password='')

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
