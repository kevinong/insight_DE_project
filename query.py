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
    command = "SELECT productid, productname from products2 LIMIT 500"
    return fetchData(command)

def getRelevantUsers(productid):
    command = "SELECT categories FROM products2 WHERE productid = \'{}\'".format(productid)
    cats = fetchData(command)[0][0]
    order = ' DESC, '.join(reversed(cats)) + ' DESC '
    command = "SELECT reviewerid FROM joined2 ORDER BY {} LIMIT 100".format(order)
    return fetchData(command)

def getUsersData(users_list):
    users_list_str = ','.join([ '\'' + t[0] + '\'' for t in users_list])
    command = "SELECT * FROM users2 WHERE reviewerid in ({}) ORDER BY count".format(users_list_str)
    return fetchData(command)


if __name__ == "__main__":
    command = "SELECT * FROM products2 LIMIT 1"
    rows = fetchData(command)
    print rows
    prod_name = rows[0][0]
    cats = rows[0][2]
    print prod_name, '\n', cats

    print ' DESC, '.join(cats) + ' DESC '

    rel_users = getRelevantUsers(prod_name)
    print(rel_users)

    user_data = getUsersData(rel_users)
    for u in user_data:
        print u

