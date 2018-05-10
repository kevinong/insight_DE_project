import configparser
import psycopg2
import sys
import os

def create_tables():
    commands = ( 
        """
        CREATE TABLE IF NOT EXISTS products (
            productid text,
            categories text[]
        );
        """
        )
    postgres_url = 'postgresql://kevin:pw@locoalhost:5432/insight'

    conn = None:

    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        # create table one by one
        print("got connection")
        for command in commands:
            cur.execute(command)
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
