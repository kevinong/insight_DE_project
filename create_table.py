from cassandra.cluster import Cluster

CASSANDRA_SERVER    = ['54.245.66.232', '54.218.181.48', '54.71.237.54', '52.13.222.70']
CASSANDRA_NAMESPACE = "AmazonReviews"

cluster = Cluster(CASSANDRA_SERVER)
session = cluster.connect()

session.execute('DROP KEYSPACE IF EXISTS '+CASSANDRA_NAMESPACE + ';')

session.execute('CREATE KEYSPACE ' + CASSANDRA_NAMESPACE  + ' WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 3};')
session.execute('USE ' + CASSANDRA_NAMESPACE)

session.execute('DROP TABLE IF EXISTS data;')
session.execute('CREATE TABLE data (reviewerID text, avg_star float, helpful float, pos float, neg float, PRIMARY KEY (reviewerID);')

