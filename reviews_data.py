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
#import products_data


BUCKET = "amazon-data-insight"
postgres_url = 'jdbc:postgresql://ec2-54-245-66-232.us-west-2.compute.amazonaws.com:5432/insight'
postgres_properties = {'user': 'kevin', 'password':'pw'}


def sentiment(reviewText):
    sentiment = TextBlob(reviewText).sentiment
    return (sentiment.polarity, sentiment.subjectivity)

def flat(input_list):
    result = []
    if input_list is None:
        return result
    for sublist in input_list:
        try:
            if sublist is not None:
                result.append(sublist[0].strip().replace(',','').replace('&', '').replace(' ', '_').lower().encode('ascii'))
                # result.append(sublist[1].strip().replace(',','').replace('&', '').replace(' ', '_').lower().encode('ascii'))
        except:
            pass

    return result

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

class ProductData:
    def __init__(self, path, conf, sc):
        sqlContext = SQLContext(sc)
        self.df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def main(self):
        self.df = self.df.select("asin", "categories").na.drop(subset=["asin"])

        print 'prod before flat \n'
        self.df.show(10, False)

        flat_udf = functions.udf(flat, ArrayType(StringType()))
        self.df = self.df.withColumn("categories", flat_udf(self.df.categories))\
                        .withColumnRenamed("asin", "productid")

        print 'prod after flat prod\n'
        self.df.show(10, False)

        self.df.write.jdbc(url=postgres_url, table='products', mode='overwrite', properties=postgres_properties)
        print 'prod saved'


class ReviewsData:
    def __init__(self, path, conf, sc):
        sqlContext = SQLContext(sc)
        self.df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def transform(self):
        # polarity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.polarity, FloatType())
        # subjectivity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.subjectivity, FloatType())

        # pos_polarity_udf = functions.udf(lambda pol: pol if pol > 0 else 0.0, FloatType())
        # neg_polarity_udf = functions.udf(lambda pol: pol if pol < 0 else 0.0, FloatType())
        
        # pos_count_udf = functions.udf(lambda pol: 1 if pol > 0 else 0, IntegerType())
        # neg_count_udf = functions.udf(lambda pol: 1 if pol < 0 else 0, IntegerType())

        self.df = self.df\
                            .withColumn("helpful_vote", self.df.helpful[0])\
                            .withColumn("unhelpful_vote", self.df.helpful[1])
                            # .withColumn("polarity", polarity_udf(self.df.reviewText))\
                            # .withColumn("subjectivity", subjectivity_udf(self.df.reviewText))\

        # self.df = self.df\
        #                     .withColumn("pos_polarity", pos_polarity_udf(self.df.polarity))\
        #                     .withColumn("neg_polarity", neg_polarity_udf(self.df.polarity))\
        #                     .withColumnRenamed("reviewerID", "reviewerid")\
        #                     .withColumn("pos_review_count", pos_count_udf(self.df.polarity))\
        #                     .withColumn("neg_review_count", neg_count_udf(self.df.polarity))

        self.user_df = self.df.groupby("reviewerid").agg(functions.count("overall").alias("avg_star"), \
                                                               functions.count("overall").alias('count'),\
                                                               functions.sum("helpful_vote").alias("helpful"), \
                                                               functions.sum("unhelpful_vote").alias("unhelpful"))
                                                               # functions.avg("polarity").alias("avg_pol"), \
                                                               # functions.sum("pos_polarity").alias("pos"), \
                                                               # functions.sum("pos_review_count").alias("pos_review_count"),\
                                                               # functions.sum("neg_polarity").alias("neg"),\
                                                               # functions.sum("neg_review_count").alias("neg_review_count"),\
                                                               # functions.avg("subjectivity").alias("subjectivity"))
                                                               # functions.collect_set("asin").alias("products"),\
                                                               # functions.collect_set("categories").alias("categories"))
        self.user_df.write.jdbc(url=postgres_url, table='users', mode='overwrite', properties=postgres_properties)


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


if __name__ == "__main__":

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')


    conf = SparkConf().setAppName("amazon").set("spark.driver.maxResultSize", "2g").set("spark.driver.memory", "3g").set("spark.sql.pivotMaxValues", 1000000)
    sc = SparkContext(conf = conf)

    sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', aws_access_key)
    sc._jsc.hadoopConfiguration().set('fs.s3a.awsSecretAccessKey',aws_secret_access_key)

    # reviews_path = get_s3_path(BUCKET, 'reviews', 'reviews_Clothing_Shoes_and_Jewelry_5.json')
    reviews_path = get_s3_path(BUCKET, 'reviews', 'complete.json')
    products_path = get_s3_path(BUCKET, "product", "metadata.json")

#    reviews_path = get_s3_path(BUCKET, "reviews", "reviews_Toys_and_Games.json")
   # products_path = get_s3_path(BUCKET, "product", "meta_Toys_and_Games.json")

    reviews_path = get_s3_path(BUCKET, "reviews", "reviews_Musical_Instruments_5.json")
    products_path = get_s3_path(BUCKET, "product", "meta_Musical_Instruments.json")

    reviewsData = ReviewsData(reviews_path, conf, sc)
    reviewsData.transform()

    productsData = ProductData(products_path, conf, sc)
    productsData.main()

    joined_df = joinDF(reviewsData.df.select("reviewerid", "asin"), productsData.df)






