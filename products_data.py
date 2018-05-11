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

BUCKET = "amazon-data-insight"
postgres_url = 'jdbc:postgresql://ec2-54-245-66-232.us-west-2.compute.amazonaws.com:5432/insight'
postgres_properties = {'user': 'kevin', 'password':'pw'}


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


if __name__ == "__main__":
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')


    conf = SparkConf().setAppName("review").set("spark.driver.maxResultSize", "2g").set("spark.driver.memory", "3g").set("spark.sql.pivotMaxValues", 1000000)
    sc = SparkContext(conf = conf)

    sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', aws_access_key)
    sc._jsc.hadoopConfiguration().set('fs.s3a.awsSecretAccessKey',aws_secret_access_key)

    #products_path = get_s3_path(BUCKET, 'product', 'meta_Toys_and_Games.json')
    products_path = get_s3_path(BUCKET, "product", "metadata.json")

    productsData = ProductData(products_path, conf, sc)
    productsData.main()
