import findspark
findspark.init()

import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

conf = SparkConf().setAppName("test")
sc = SparkContext(conf = conf)

bucket_name = "amazon-data-insight"
folder_name = "product"
file_name = "metadata.json"
path = 's3a://{}/{}/{}/'.format(bucket_name, folder_name, file_name)

sqlContext = SQLContext(sc)

df = sqlContext.read.format('json').\
        options(header='true', inferSchema='true').\
        load(path)

print("count: ", df.count())
print("dtypes: ", df.dtypes)
print("first row: ", df.first())
