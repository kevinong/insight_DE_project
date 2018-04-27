import findspark
findspark.init()

import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

BUCKET = "amazon-data-insight"

def get_s3_path(bucket_name, folder_name, file_name):
    return 's3a://{}/{}/{}/'.format(bucket_name, folder_name, file_name)


if __name__ == "__main__":

    # aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    # aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

    conf = SparkConf().setAppName("test")
    sc = SparkContext(conf = conf)

    product_path = get_s3_path(BUCKET, "product", "metadata.json")

    sqlContext = SQLContext(sc)

    df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(product_path)

    print "count: ", df.count()
    print ''
    print "dtypes: ", df.dtypes
    print ''
    print "first 5 rows: ", df.head(5)
    print ''

    books_df = df.where('books'.encode('ascii') in df['categories'][0])
    print "first 5 book rows", books_df.head(5)
