import findspark
findspark.init()

import os
# from pyspark import SparkConf, SparkContext
import pyspark
from pyspark.sql import sqlContext

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

# conf = SparkConf().setAppName("test")
# sc = SparkContext(conf = conf)


# path = "s3n://{}:{}@/product/metadata.json"
# data = sc.textFile(path)
bucket_name = "amazon-data-insight"
folder_name = "product"
file_name = "metadata.json"

EC=sqlContext.read.format('json').options(header='true', inferSchema='true').load('s3a://{}/{}/{}/'.format(bucket_name, folder_name, file_name))
# print(data)
print(EC)