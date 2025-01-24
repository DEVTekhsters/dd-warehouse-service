# ITs only for testing use of the connection

import clickhouse_connect
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

class Connection:
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=os.getenv('CLICKHOUSE_PORT'),
        username=os.getenv('CLICKHOUSE_USERNAME'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DATABASE')
    )