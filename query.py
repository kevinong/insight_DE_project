import psycopg2

postgres_url = 'postgresql://kevin:pw@ec2-54-245-66-232.us-west-2.compute.amazonaws.com:5432/insight'

def select(command):
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        # create table one by one
        print("got connection")
       # for command in commands:
        print command
        cur.execute(command)
        print("executed command")
        # close communication with the PostgreSQL database server
        rows = cur.fetchall()
        print("fetched data")
        cur.close()
        print("closed the cursor")
        # commit the changes
        # conn.commit()
        # print("committed the connection")
    except (Exception) as error:
        print(error)
        raise error
    finally:
        if conn is not None:
            conn.close()
            print("closed the connection")
     return rows

if __name__ == "__main__":
    command = "SELECT * FROM products LIMIT 1"
    rows = select(command)
    print rows
    prod_name = rows[0][0]
    cats = rows[0][1]
    print prod_name, '\n', cats

    command = "SELECT * FROM joined ORDER BY {} DESC, {} DESC LIMIT 10".format(cats[0], cats[1])
    user_rows = select(command)
    for r in user_rows:
        print r

# from cassandra.cluster import Cluster

# CASSANDRA_SERVER    = ['54.245.66.232', '54.218.181.48', '54.71.237.54', '52.13.222.70']
# CASSANDRA_NAMESPACE = "AmazonReviews"

# cluster = Cluster(CASSANDRA_SERVER)
# session = cluster.connect()

# session.execute("USE " + CASSANDRA_NAMESPACE)

# product_rows = session.execute('SELECT * FROM productData')
# cat = product_rows[0]
# # user_cat_rows = session.execute('SELECT * FROM joineddata')

# # +product_rows = session.execute('SELECT * FROM productData LIMIT 5')
# # +print product_rows[0]
# # +cat = product_rows[0].categories[1].strip().replace(',','').replace('&', '').replace(' ', '_').lower().encode('ascii')
# # +print cat
# # +user_cat_rows = session.execute('SELECT * FROM joineddata WHERE {} > 5 ALLOW FILTERING'.format(cat))
# # +
# # +for r in user_cat_rows:
# # +    print r