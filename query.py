import psycopg2
postgres_url = 'postgresql://kevin:pw@ec2-54-245-66-232.us-west-2.compute.amazonaws.com:5432/insight'

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

def getAllProducts():
    command = "SELECT productid from products"
    return fetchData(command)

def getRelevantUsers(productid):
    command = "SELECT categories FROM products WHERE productid = \'{}\'".format(productid)
    cats = fetchData(command)[0][0]
    print 'in rel user ', cats
    command = "SELECT reviewerid FROM joined ORDER BY {} DESC, {} DESC LIMIT 10".format(cats[0],cats[1])
    return fetchData(command)

def getUsersData(users_list):
    print users_list
    print type(users_list)
    
    users_list_str = ','.join([ '\'' + t[0] + '\'' for t in users_list])
    command = "SELECT * FROM users WHERE reviewerid in ({}) ORDER BY count".format(users_list_str)
    return fetchData(command)

if __name__ == "__main__":
    command = "SELECT * FROM products LIMIT 1"
    rows = fetchData(command)
    print rows
    prod_name = rows[0][0]
    cats = rows[0][1]
    print prod_name, '\n', cats

    rel_users = getRelevantUsers(prod_name)
    print(rel_users)

    user_data = getUsersData(rel_users)
    for u in user_data:
        print u

