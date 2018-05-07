import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType, IntegerType

from textblob import TextBlob
import datetime

# from boto.s3.connection import S3Connection


BUCKET = "amazon-data-insight"

def get_s3_path(bucket_name, folder_name=None, file_name=None):
    path = 's3a://{}/'.format(bucket_name)

    if folder_name is not None:
        path += '{}/'.format(folder_name)
    if file_name is not None:
        path += '{}/'.format(file_name)

    return path

def sentiment(reviewText):
    sentiment = TextBlob(reviewText).sentiment
    return (sentiment.polarity, sentiment.subjectivity)

class ReviewsData:
    def __init__(self, path, conf, sc):
        sqlContext = SQLContext(sc)
        self.reviews_df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def main(self):

        self.reviews_df.show(5)

        print "start time: ", datetime.datetime.now()

        polarity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.polarity, FloatType())
        subjectivity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.subjectivity, FloatType())

        pos_polarity_udf = functions.udf(lambda pol: pol if pol > 0 else 0.0, FloatType())
        neg_polarity_udf = functions.udf(lambda pol: pol if pol < 0 else 0.0, FloatType())
        
        pos_count_udf = functions.udf(lambda pol: 1 if pol > 0 else 0, IntegerType())
        neg_count_udf = functions.udf(lambda pol: 1 if pol < 0 else 0, IntegerType())

        print "start transform 1: ", datetime.datetime.now()
        # Transforming review data
        self.reviews_df = self.reviews_df\
                            .withColumn("polarity", polarity_udf(self.reviews_df.reviewText))\
                            .withColumn("subjectivity", subjectivity_udf(self.reviews_df.reviewText))\
                            .withColumn("helpful_vote", self.reviews_df.helpful[0])\
                            .withColumn("unhelpful_vote", self.reviews_df.helpful[1])

        self.reviews_df.show(5)
        print "start transfrom 2: ", datetime.datetime.now()

        self.reviews_df = self.reviews_df\
                            .withColumn("pos_polarity", pos_polarity_udf(self.reviews_df.polarity))\
                            .withColumn("neg_polarity", neg_polarity_udf(self.reviews_df.polarity))\
                            .withColumnRenamed("reviewerID", "reviewerid")\
                            .withColumn("pos_review_count", pos_count_udf(self.reviews_df.polarity))\
                            .withColumn("neg_review_count", neg_count_udf(self.reviews_df.polarity))



        print 'transformation done: ', datetime.datetime.now()
        self.reviews_df.show(5)

        grouped_df = self.reviews_df.groupby("reviewerid").agg(functions.avg("overall").alias("avg_star"), \
                                                               functions.sum("helpful_vote").alias("helpful"), \
                                                               functions.sum("unhelpful_vote").alias("unhelpful"), \
                                                               functions.avg("polarity").alias("avg_pol"), \
                                                               functions.sum("pos_polarity").alias("pos"), \
                                                               functions.sum("pos_review_count"),\
                                                               functions.sum("neg_polarity").alias("neg"),\
                                                               functions.sum("neg_review_count"),\
                                                               functions.avg("subjectivity").alias("subjectivity"),\
                                                               functions.collect_set("asin").alias("products"))

        grouped_df.show(20)

        # grouped_df.write.format("org.apache.spark.sql.cassandra").options(table = "data", keyspace = "amazonreviews").save()

        # table1 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="kv", keyspace="ks").load()
        # table1.write.format("org.apache.spark.sql.cassandra").options(table="othertable", keyspace = "ks").save(mode ="append")


def flat(cat):
    res = []
    if cat is None:
        return res
    for c in cat:
        if c is not None and c not in res:
            res += c

    return res

class ProductData:
    def __init__(self, path, conf, sc):
        sqlContext = SQLContext(sc)
        self.df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def main(self):
        print self.df.select("categories").dtypes
        self.df.select("categories").show(10, False)

        flat_udf = functions.udf(flat, ArrayType(StringType()))
        new_df = self.df.withColumn("categories", flat_udf(self.df.categories))
        new_df.select("categories").show(10, False)
        new_df = new_df.withColumn("related", flat_udf(self.df.related))
        new_df.select("related").show(10, False)

        # self.df.select("categories").rdd.map(lambda row:(row[0], reduce(lambda x,y:x+y, row[1]))).toDF().show(10)

        # flattenUdf = functions.udf(fudf, ArrayType(StringType()))
        # self.df.select(flattenUdf("categories").alias("categories")).show(10)

        # flatten = lambda l: [item for sublist in l for item in sublist]
        # flatlist_udf = functions.udf(lambda categories: [item for sublist in categories for item in sublist], ArrayType(StringType()))
        # self.df = self.df.withColumn("categories", flattenUdf(self.df.categories))
        


        # self.df = self.df.withColumn("cat2", flatten_udf(self.df.categories))

        # self.df.show(10)

        # new_df = self.df.select("categories").rdd.map(lambda val: reduce(custom, val)).toDF()
        # new_df = self.df.select("categories").rdd.map(lambda val: val[0]).toDF()
        # print new_df.dtypes
        # new_df.select('_1').show(10)


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

    reviews_path = get_s3_path(BUCKET, 'reviews', 'reviews_Clothing_Shoes_and_Jewelry_5.json')
    # products_path = get_s3_path(BUCKET, 'product', 'meta_Toys_and_Games.json')
    products_path = get_s3_path(BUCKET, "product", "metadata.json")

    # productsData = ProductData(products_path, conf, sc)
    # productsData.main()


    reviewsData = ReviewsData(reviews_path, conf, sc)
    reviewsData.main()


    # product_path = get_s3_path(BUCKET, "product", "metadata.json")
    # qa_path = get_s3_path(BUCKET, 'qa', 'qa_Appliances.json')
    # # reviews_path = get_s3_path(BUCKET, 'reviews', 'complete.json')
    # reviews_path = get_s3_path(BUCKET, 'reviews', 'reviews_Books_5.json')

    # sqlContext = SQLContext(sc)

    # # add_tuple_udf = functions.udf(add_tuple)

    # # product_df = sqlContext.read.format('json').\
    # #         options(header='true', inferSchema='true').\
    # #         load(product_path)

    # # qa_df = sqlContext.read.format('json').\
    # #         options(header='true', inferSchema='true').\
    # #         load(qa_path)

    # reviews_df = sqlContext.read.format('json').\
    #         options(header='true', inferSchema='true').\
    #         load(reviews_path)


    # print reviews_df.dtypes

    # print "agg \n"

    # sentiment_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.polarity, FloatType())

    # # print reviews_df.groupby("reviewerID").agg(F.avg("overall"), F.min("overall"), F.max("overall"), F.count("overall")).show(50)

    # # reviews_df = reviews_df.withColumn("helpful", reviews_df.helpful[0] - reviews_df.helpful[1])
    # # print reviews_df.show(10)
    # # print reviews_df.groupby("reviewerID").agg(F.avg("helpful"), F.min("helpful"), F.max("helpful"), F.count("helpful"),).show(50)

    # # reviews_df = reviews_df.withColumn("reviewText", TextBlob(reviews_df.reviewText).sentiment.polarity)
    # # reviews_df['sentiment'] = reviews_df['reviewText'].apply(lambda review: TextBlob(review).sentiment.polarity)
    # # reviews_df['sentiment'] = reviews_df['reviewText'].apply(sentiment_calc)
    # reviews_df = reviews_df.withColumn("sentiment", sentiment_udf(reviews_df.reviewText))
    # print reviews_df.show(10)
    # print reviews_df.groupby("reviewerID").agg(functions.avg("sentiment"), functions.min("sentiment"), functions.max("sentiment"), functions.count("sentiment")).show(50)

    # '''
    # TODO:
    # refactor code. rename test.py. make a class ReviewDataTransformer()
    # when agg user data, add all neg reviews and all pos reviews separately to correctly capture the sentiment
    # '''





