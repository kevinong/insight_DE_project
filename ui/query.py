import psycopg2

postgres_url = os.getenv('POSTGRES_URL', 'default')

def fetchData(command):
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        cur.execute(command)
        rows = cur.fetchall()
        cur.close()
    except (Exception) as error:
        print(error)
        raise error
    finally:
        if conn is not None:
            conn.close()
    return rows

def getAllProducts(cat):
    command = "SELECT productid, productname from {} ".format(cat + "products")
    return fetchData(command)

def getRelevantUsers(productid, cat):
    # command = "SELECT categories FROM products2 WHERE productid = \'{}\'".format(productid)
    command = "SELECT categories FROM {} WHERE productid = \'{}\'".format(cat + "products", productid)
    cats = fetchData(command)[0][0]
    order = ' DESC, '.join(reversed(cats)) + ' DESC '
    command = "SELECT reviewerid FROM {} ORDER BY {} LIMIT 100".format(cat + "joined", order)
    return fetchData(command)

def getUsersData(users_list, cat):
    users_list_str = ','.join([ '\'' + t[0] + '\'' for t in users_list])
    command = "SELECT * FROM {} WHERE reviewerid in ({}) ORDER BY count".format(cat + "users", users_list_str)
    return fetchData(command)



