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

    reviews_rdd = sc.textfile(reviews_path)

    print reviews_rdd.take(10)

    # sqlContext = SQLContext(sc)

    # add_tuple_udf = F.udf(add_tuple)

    # product_df = sqlContext.read.format('json').\
    #         options(header='true', inferSchema='true').\
    #         load(product_path)

    # qa_df = sqlContext.read.format('json').\
    #         options(header='true', inferSchema='true').\
    #         load(qa_path)

    # reviews_df = sqlContext.read.format('json').\
    #         options(header='true', inferSchema='true').\
    #         load(reviews_path)

    # print "prod count: \n", product_df.count()
    # print ''
    # print "prod dtypes: \n", product_df.dtypes
    # print ''
    # print "first 5 rows (prod): \n", product_df.show(5)
    # print ''

    # print "qa count: \n", qa_df.count()
    # print ''
    # print "qa dtypes: \n", qa_df.dtypes
    # print ''
    # print "first 5 rows (qa): \n", qa_df.show(5)
    # print ''

    # print "Group by product: "
    # print qa_df.groupby("asin").count().show()

    # print "first 5 rows (reviews): \n", reviews_df.show(20)

    # print reviews_df.dtypes

    # print "group by users: \n", 
    # grouped = reviews_df.groupby("reviewerID")
    # print "grouped: \n", grouped.show(20)

    # print "agg \n"

    # print reviews_df.groupby("reviewerID").agg(F.avg("overall"), F.min("overall"), F.max("overall"), F.count("overall")).show(50)

    # print reviews_df.select("reviewerID", "helpful") # returns a dataframe
    # print reviews_df.select("reviewerID", "helpful").rdd.map(lambda x: x.split(' '))






