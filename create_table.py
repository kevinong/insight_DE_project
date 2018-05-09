from cassandra.cluster import Cluster

CASSANDRA_SERVER    = ['54.245.66.232', '34.214.245.150', '54.218.181.48', '54.71.237.54', '54.190.226.253', '35.165.118.115', '52.11.177.167', '34.215.123.166']
CASSANDRA_NAMESPACE = "amazonreviews"

cluster = Cluster(CASSANDRA_SERVER)
session = cluster.connect()

session.execute('DROP KEYSPACE IF EXISTS '+CASSANDRA_NAMESPACE + ';')

session.execute('CREATE KEYSPACE ' + CASSANDRA_NAMESPACE  + ' WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 3};')
session.execute('USE ' + CASSANDRA_NAMESPACE)

session.execute('DROP TABLE IF EXISTS userdata;')
session.execute('CREATE TABLE userdata (reviewerid text, avg_star float, helpful int, unhelpful int, avg_pol float, pos float, pos_review_count int, neg float, neg_review_count int, subjectivity float, PRIMARY KEY (reviewerid));')

# grouped_df = self.df.groupby("reviewerid").agg(functions.avg("overall").alias("avg_star"), \
#                                                                functions.sum("helpful_vote").alias("helpful"), \
#                                                                functions.sum("unhelpful_vote").alias("unhelpful"), \
#                                                                functions.avg("polarity").alias("avg_pol"), \
#                                                                functions.sum("pos_polarity").alias("pos"), \
#                                                                functions.sum("pos_review_count"),\
#                                                                functions.sum("neg_polarity").alias("neg"),\
#                                                                functions.sum("neg_review_count"),\
#                                                                functions.avg("subjectivity").alias("subjectivity"),\
#                                                                functions.collect_set("asin").alias("products"),\
#                                                                functions.collect_set("categories"))
