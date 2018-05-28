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
postgres_user = os.getenv('POSTGRES_USER', 'default')
postgres_pw = os.getenv('POSTGRES_PW', 'default')
postgres_properties = {'user': postgres_user, 'password':postgres_pw}


def flat(input_list):
    result = []
    if input_list is None:
        return result
    for sublist in input_list:
        try:
            if sublist is not None:
                result.append(sublist[0].strip().replace(',','').replace('&', '').replace(' ', '_').lower().encode('ascii'))
        except:
            pass

    return result

class ProductData:
    def __init__(self, category, conf, sc):
        path = get_s3_path(BUCKET, "product", "meta_{}.json".format(CATEGORIES[category]))
        self.category = category
        sqlContext = SQLContext(sc)
        self.df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def main(self):
        self.df = self.df.select("asin", "title", "categories").na.drop(subset=["asin"])

        flat_udf = functions.udf(flat, ArrayType(StringType()))
        self.df = self.df.withColumn("categories", flat_udf(self.df.categories))\
                        .withColumnRenamed("asin", "productid")\
                        .withColumnRenamed("title", "productname")

        self.df.write.jdbc(url=postgres_url, table='{}products'.format(self.category), mode='overwrite', properties=postgres_properties)


