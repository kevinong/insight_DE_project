import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType, IntegerType
from cassandra.cluster import Cluster

import configparser
import psycopg2
import sys

import os
from textblob import TextBlob
import datetime

from util import get_s3_path

def flat2(input_list):
    result = []
    if input_list is None:
        return result
    for sublist in input_list:
        try:
            if sublist is not None:
                for val in sublist:
                    result.append(val.strip().replace(',','').replace('&', '').replace(' ', '_').lower().encode('ascii'))
                # result.append(sublist[1].strip().replace(',','').replace('&', '').replace(' ', '_').lower().encode('ascii'))
        except:
            pass

    return result

def createTable(distincts):
    fields = " int, ".join(distincts) + ' int'
    print 'CREATING TABLE\n'
    print fields

    command = "CREATE TABLE IF NOT EXISTS joined (reviewerid text PRIMARY KEY, {});".format(fields)

    postgres_url = 'postgresql://kevin:pw@ec2-54-245-66-232.us-west-2.compute.amazonaws.com:5432/insight'

    conn = None

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

def joinDF(rev_df, prod_df):
    print 'prod before join'
    prod_df.show(10, False)
    joined_df = rev_df.join(prod_df, rev_df.asin == prod_df.productid)

    joined_df = joined_df.groupby("reviewerid").agg(functions.collect_list("categories").alias("categories"))

    print 'first join'
    joined_df.show(10)

    flat2_udf = functions.udf(flat2, ArrayType(StringType()))
    joined_df = joined_df.withColumn("categories", flat2_udf(joined_df.categories))

    print 'joined after flat2'
    joined_df.show(10)

    joined_df = joined_df.rdd.flatMap(lambda (user, cats) : [(user, cat) for cat in cats]).toDF(["reviewerid", "category"])

    print 'joined after flatmap'
    joined_df.show(10)

    print "distinct cats: \n", joined_df.select("category").distinct().count()
    
    distincts = [i.category for i in joined_df.select('category').distinct().collect()]
    print "DISTINCT CATEGORIES \n"
    print distincts

    joined_df = joined_df.groupby("reviewerid").pivot("category").count()
    joined_df = joined_df.na.fill(0)

    createTable(distincts)
    # joined_df = joined_df.drop("categories")
    # joined_df.printSchema()
    joined_df.show(10)
    # joined_df.write.format("org.apache.spark.sql.cassandra").mode('overwrite').options(table = "joineddata", keyspace = "amazonreviews").save()
    joined_df.write.jdbc(url=postgres_url, table='joined', mode='overwrite', properties=postgres_properties)


    return joined_df

if __name__ == "__main__":
