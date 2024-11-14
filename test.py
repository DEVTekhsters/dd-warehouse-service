import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', username='default', password='')
# client.command('CREATE TABLE new_table (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key')
# print(client)

result = client.query('SELECT max(key), avg(metric) FROM new_table')
print(result.result_rows)


