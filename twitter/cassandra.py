import cassandra
from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)
 
## TO-DO: Create the keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS tweets_sentiments
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )

except Exception as e:
    print(e)

## To-Do: Add in the keyspace you created
try:
    session.set_keyspace('tweets_sentiments')
except Exception as e:
    print(e)
query = "CREATE TABLE IF NOT EXISTS sentiments_table"
query = query + "(created_at text, timestamp text, cleaned_data text, message text, City text, Temp int, prediction int, PRIMARY KEY ((created_at, message), City)"
try:
    session.execute(query)
except Exception as e:
    print(e)
