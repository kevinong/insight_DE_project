import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions as F
from textblob import TextBlob

# from boto.s3.connection import S3Connection


BUCKET = "amazon-data-insight"

def get_s3_path(bucket_name, folder_name=None, file_name=None):
    path = 's3a://{}/'.format(bucket_name)

    if folder_name is not None:
        path += '{}/'.format(folder_name)
    if file_name is not None:
        path += '{}/'.format(file_name)

    return path

def add_tuple(tup):
    return tup[0] + tup[1]

if __name__ == "__main__":

    # aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    # aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
    # fs = s3fs.S3FileSystem(anon = True)
    # print(fs.ls('s3a://{}/{}/'.format(BUCKET, 'qa')))

    # conn = S3Connection('access-key','secret-access-key')
    # bucket = conn.get_bucket('bucket')
    # for key in bucket.list():
    #     print key.name.encode('utf-8')

    conf = SparkConf().setAppName("test")
    sc = SparkContext(conf = conf)

    product_path = get_s3_path(BUCKET, "product", "metadata.json")
    qa_path = get_s3_path(BUCKET, 'qa', 'qa_Appliances.json')
    # reviews_path = get_s3_path(BUCKET, 'reviews', 'complete.json')
    reviews_path = get_s3_path(BUCKET, 'reviews', 'reviews_Books_5.json')

    sqlContext = SQLContext(sc)

    # add_tuple_udf = F.udf(add_tuple)

    # product_df = sqlContext.read.format('json').\
    #         options(header='true', inferSchema='true').\
    #         load(product_path)

    # qa_df = sqlContext.read.format('json').\
    #         options(header='true', inferSchema='true').\
    #         load(qa_path)

    reviews_df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(reviews_path)


    print reviews_df.dtypes

    print "agg \n"

    # print reviews_df.groupby("reviewerID").agg(F.avg("overall"), F.min("overall"), F.max("overall"), F.count("overall")).show(50)

    reviews_df = reviews_df.withColumn("helpful", reviews_df.helpful[0] - reviews_df.helpful[1])
    print reviews_df.show(10)
    print reviews_df.groupby("reviewerID").agg(F.avg("helpful"), F.min("helpful"), F.max("helpful"), F.count("helpful"),).show(50)

    reviews_df = reviews_df.withColumn("reviewText", TextBlob(reviews_df.reviewsText).sentiment.polarity)
    print reviews_df.show(10)
    print reviews_df.groupby("reviewerID").agg(F.avg("reviewText"), F.min("reviewText"), F.max("reviewText"), F.count("reviewText")).show(50)






