# ITs only for  tesing use of the connection 

import clickhouse_connect
                            
class Connection:
   
    client = clickhouse_connect.get_client(
            host='148.113.6.50',
            port="8123",
            username='default',
            password='',
            database='default'
        )
    
