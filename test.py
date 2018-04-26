import os
from pyspark import SparkConf, SparkContext

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

conf = SparkConf().setAppName("test")
sc = SparkContext(conf = conf)


path = "s3n://{}:{}@/product/metadata.json"
data = sc.textFile(path)

print(data)