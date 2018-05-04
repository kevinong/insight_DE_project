from cassandra.cluster import Cluster

CASSANDRA_SERVER    = ['54.245.66.232', '54.218.181.48', '54.71.237.54', '52.13.222.70']
CASSANDRA_NAMESPACE = "AmazonReviews"

cluster = Cluster(CASSANDRA_SERVER)
session = cluster.connect()

session.execute("USE " + CASSANDRA_NAMESPACE)

rows = session.execute('SELECT reviewerid, avg_star FROM data')

counter = 0
for user_row in rows:
    if counter > 5:
        break
    counter += 1
    print user_row.reviewerID, user_row.avg_star