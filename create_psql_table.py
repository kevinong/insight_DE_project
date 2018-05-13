import configparser
import psycopg2
import sys
import os

def create_tables():

    cats = ['books', 'electronics', 'moviestv', 'cdsvinyl', 'clothingshoesjewelry', 'homekitchen', 'kindlestore', 'sportsoutdoors', 'cellphonesaccessories', 'healthpersonalcare', 'toysgames', 'videogames', 'toolshomeimprovement', 'beauty', 'appsforandroid', 'officeproducts', 'petsupplies', 'automotive', 'grocerygourmetfood', 'patiolawngarden', 'baby', 'digitalmusic', 'musicalinstruments', 'amazoninstantvideo']

    
    # command2 = "CREATE TABLE IF NOT EXISTS users2 (reviewerid text PRIMARY KEY, avg_star float, count int, helpful int, unhelpful int);"

    postgres_url = 'postgresql://kevin:pw@ec2-54-245-66-232.us-west-2.compute.amazonaws.com:5432/insight'

    conn = None

    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        # create table one by one
        print("got connection")
       # for command in commands:
       # print command
        for c in cats:
            command1 = "CREATE TABLE IF NOT EXISTS " + c + "products (productid text PRIMARY KEY, productname text, categories text[]);"
            command2 = "CREATE TABLE IF NOT EXISTS " + c + "users (reviewerid text PRIMARY KEY, avg_star float, count int, helpful int, unhelpful int, avg_pol float, pos float, pos_review_count int, neg float, neg_review_count int, subjectivity float);"
            cur.execute(command1)
            cur.execute(command2)
            print("executed command")
        # close communication with the PostgreSQL database server
        cur.close()
        print("closed the cursor")
        # commit the changes
        conn.commit()
        print("committed the connection")
    except (Exception) as error:
        print(error)
        raise error
    finally:
        if conn is not None:
            conn.close()
            print("closed the connection")
            
if __name__ == '__main__':
    create_tables()
