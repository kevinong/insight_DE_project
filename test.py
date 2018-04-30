import findspark
findspark.init()

import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import s3fs

BUCKET = "amazon-data-insight"

def get_s3_path(bucket_name, folder_name, file_name):
    return 's3a://{}/{}/{}/'.format(bucket_name, folder_name, file_name)


if __name__ == "__main__":

    # aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    # aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
    # fs = s3fs.S3FileSystem(anon = True)
    # print(fs.ls('s3a://{}/{}/'.format(BUCKET, 'qa')))

    conf = SparkConf().setAppName("test")
    sc = SparkContext(conf = conf)

    product_path = get_s3_path(BUCKET, "product", "metadata.json")
    qa_path = get_s3_path(BUCKET, 'qa', 'qa_Appliances.json')
    reviews_path = get_s3_path(BUCKET, 'reviews', 'complete.json')

    sqlContext = SQLContext(sc)

    product_df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(product_path)

    qa_df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(qa_path)

    reviews_df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(reviews_path)

    print "prod count: \n", product_df.count()
    print ''
    print "prod dtypes: \n", product_df.dtypes
    print ''
    print "first 5 rows (prod): \n", product_df.show(5)
    print ''

    print "qa count: \n", qa_df.count()
    print ''
    print "qa dtypes: \n", qa_df.dtypes
    print ''
    print "first 5 rows (qa): \n", qa_df.show(5)
    print ''

    print "Group by product: "
    print qa_df.groupby("asin").count().show()

    print "first 5 rows (reviews): \n", reviews_df.show(5)



