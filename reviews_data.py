import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType, IntegerType
from cassandra.cluster import Cluster

import os
from textblob import TextBlob
import datetime

from util import get_s3_path
#import products_data


BUCKET = "amazon-data-insight"

def sentiment(reviewText):
    sentiment = TextBlob(reviewText).sentiment
    return (sentiment.polarity, sentiment.subjectivity)

def flat(input_list):
    result = []
    if input_list is None:
        return result
    for sublist in input_list:
        try:
            if sublist is not None:
                result.append(sublist[0].replace(',','').replace('&', '').replace(' ', '_').lower().encode('ascii'))
                result.append(sublist[1].replace(',','').replace('&', '').replace(' ', '_').lower().encode('ascii'))
                # for val in sublist:
                #     if val is not None and val not in result:
                #         result.append(val)
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
        # self.df = self.df.na.drop(subset=["asin"])
        # self.df.select("categories").show(20, False)
        flat_udf = functions.udf(flat, ArrayType(StringType()))

        self.df = self.df.withColumn("categories", flat_udf(self.df.categories))\
                        .withColumnRenamed("asin", "productid")

        # self.df.select("categories").show(20, False)

        #self.df.write.format("org.apache.spark.sql.cassandra").mode('overwrite').options(table = "productdata", keyspace = "amazonreviews").save()

        # self.df.select("categories").show(10, False)

        # self.df = self.df.withColumn("related", flat_udf(self.df.related))
        # self.df.select("related").show(10, False)


class ReviewsData:
    def __init__(self, path, conf, sc):
        sqlContext = SQLContext(sc)
        self.df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def transform(self):

        # self.df.show(5)

        print "start time: ", datetime.datetime.now()

        polarity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.polarity, FloatType())
        subjectivity_udf = functions.udf(lambda reviewText: TextBlob(reviewText).sentiment.subjectivity, FloatType())

        pos_polarity_udf = functions.udf(lambda pol: pol if pol > 0 else 0.0, FloatType())
        neg_polarity_udf = functions.udf(lambda pol: pol if pol < 0 else 0.0, FloatType())
        
        pos_count_udf = functions.udf(lambda pol: 1 if pol > 0 else 0, IntegerType())
        neg_count_udf = functions.udf(lambda pol: 1 if pol < 0 else 0, IntegerType())

        print "start transform 1: ", datetime.datetime.now()
        # Transforming review data
        self.df = self.df\
                            .withColumn("polarity", polarity_udf(self.df.reviewText))\
                            .withColumn("subjectivity", subjectivity_udf(self.df.reviewText))\
                            .withColumn("helpful_vote", self.df.helpful[0])\
                            .withColumn("unhelpful_vote", self.df.helpful[1])

        # self.df.show(5)
        print "start transfrom 2: ", datetime.datetime.now()

        self.df = self.df\
                            .withColumn("pos_polarity", pos_polarity_udf(self.df.polarity))\
                            .withColumn("neg_polarity", neg_polarity_udf(self.df.polarity))\
                            .withColumnRenamed("reviewerID", "reviewerid")\
                            .withColumn("pos_review_count", pos_count_udf(self.df.polarity))\
                            .withColumn("neg_review_count", neg_count_udf(self.df.polarity))

        print 'transformation done: ', datetime.datetime.now()

        self.user_df = self.df.groupby("reviewerid").agg(functions.avg("overall").alias("avg_star"), \
                                                               functions.count("overall").alias('count'),\
                                                               functions.sum("helpful_vote").alias("helpful"), \
                                                               functions.sum("unhelpful_vote").alias("unhelpful"), \
                                                               functions.avg("polarity").alias("avg_pol"), \
                                                               functions.sum("pos_polarity").alias("pos"), \
                                                               functions.sum("pos_review_count").alias("pos_review_count"),\
                                                               functions.sum("neg_polarity").alias("neg"),\
                                                               functions.sum("neg_review_count").alias("neg_review_count"),\
                                                               functions.avg("subjectivity").alias("subjectivity"))
                                                               # functions.collect_set("asin").alias("products"),\
                                                               # functions.collect_set("categories").alias("categories"))
# session.execute('CREATE TABLE data (reviewerid text,
# avg_star float, helpful int, unhelpful int, avg_pol float, pos float, pos_review_count int, neg float, neg_review_count int, subjectivity float, PRIMARY KEY (reviewerid));')


        # print 'group done'
#        grouped_df.write.format("txt").save("df.txt")
#        grouped_df.rdd.saveAsTextFile("df.txt")
 #       grouped_df.show(20)

        self.grouped_df.write.format("org.apache.spark.sql.cassandra").mode('overwrite').options(table = "userdata", keyspace = "amazonreviews").save()

        # table1 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="kv", keyspace="ks").load()
        # table1.write.format("org.apache.spark.sql.cassandra").options(table="othertable", keyspace = "ks").save(mode ="append")

def getCat(cat):
    result = []
    try:
        result.append(cat[0])
#        result.append(cat[1])
    except:
        return result

    return result

def joinDF(rev_df, prod_df):

    # getCat_udf = functions.udf(getCat, ArrayType(StringType()))
    # prod_df = prod_df.withColumnRenamed('asin', 'asin2')
                     # .withColumn("categories", getCat_udf(prod_df.categories))
    print 'show truncated prod'
    prod_df.show(10, False)
    joined_df = rev_df.join(prod_df, rev_df.asin == prod_df.productid)

    joined_df = joined_df.groupby("reviewerid").agg(functions.collect_list("categories").alias("categories"))

    flat_udf = functions.udf(flat, ArrayType(StringType()))
    joined_df = joined_df.withColumn("categories", flat_udf(joined_df.categories))
    joined_df = joined_df.rdd.flatMap(lambda (user, cats) : [(user, cat) for cat in cats]).toDF(["reviewerid", "category"])
    print "distinct cats: \n", joined_df.select("category").distinct().count()
    # distincts = joined_df.select("category").distinct().collect()

   # chars = re.escape(string.punctuation)
   # print re.sub(r'['+chars+']', '',my_str)
    distincts = [i.category for i in joined_df.select('category').distinct().collect()]
    print "DISTINCT CATEGORIES \n"
    print distincts

    joined_df = joined_df.groupby("reviewerid").pivot("category").count()
    joined_df = joined_df.na.fill(0)

    createTable(distincts)
    joined_df = joined_df.drop("categories")
    joined_df.show(10)
    joined_df.write.format("org.apache.spark.sql.cassandra").mode('overwrite').options(table = "joineddata", keyspace = "amazonreviews").save()


    return joined_df

def createTable(distincts):
    CASSANDRA_SERVER    = ['54.245.66.232', '34.214.245.150', '54.218.181.48', '54.71.237.54', '54.190.226.253', '35.165.118.115', '52.11.177.167', '34.215.123.166']
    CASSANDRA_NAMESPACE = "amazonreviews"

    cluster = Cluster(CASSANDRA_SERVER)
    session = cluster.connect()

    session.execute('USE ' + CASSANDRA_NAMESPACE)
    session.execute('DROP TABLE IF EXISTS joineddata;')
    # distincts = [i.category.replace(' ', '_') for i in joined_df.select('category').distinct().collect()]
    # print distincts
    fields = " int ,".join(distincts) + ' int'
    print 'CREATING TABLE\n'
    print fields

    cql_command = 'CREATE TABLE joineddata (reviewerid text, {}, PRIMARY KEY (reviewerid));'.format(fields)
    print 'CQL COMMAND \n'
    print cql_command
    session.execute(cql_command)


if __name__ == "__main__":

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
    # fs = s3fs.S3FileSystem(anon = True)
    # print(fs.ls('s3a://{}/{}/'.format(BUCKET, 'qa')))


    conf = SparkConf().setAppName("test").set("spark.driver.maxResultSize", "2g").set("spark.driver.memory", "3g").set("spark.sql.pivotMaxValues", 1000000)
    sc = SparkContext(conf = conf)
    # sc.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", aws_access_key)
    # sc.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", aws_secret_access_key)

    sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', aws_access_key)
    sc._jsc.hadoopConfiguration().set('fs.s3a.awsSecretAccessKey',aws_secret_access_key)

    # reviews_path = get_s3_path(BUCKET, 'reviews', 'reviews_Clothing_Shoes_and_Jewelry_5.json')
    reviews_path = get_s3_path(BUCKET, 'reviews', 'complete.json')
    products_path = get_s3_path(BUCKET, "product", "metadata.json")

#    reviews_path = get_s3_path(BUCKET, "reviews", "reviews_Toys_and_Games.json")
   # products_path = get_s3_path(BUCKET, "product", "meta_Toys_and_Games.json")

    reviews_path = get_s3_path(BUCKET, "reviews", "reviews_Musical_Instruments_5.json")
    products_path = get_s3_path(BUCKET, "product", "meta_Musical_Instruments.json")

    productsData = ProductData(products_path, conf, sc)
    productsData.main()
#    productsData.df

    # prod_df = productsData.df.select("asin", "categories")
    # print 'show product data'
    # prod_df.show(10, False)

    reviewsData = ReviewsData(reviews_path, conf, sc)
    # reviewsData.main()

    # print 'show review data'
    # reviewsData.df.select("reviewerID", "asin").show(10)

    # print 'show joined data'
    reviewsData.df = reviewsData.df.withColumnRenamed("reviewerID", "reviewerid")
    joined_df = joinDF(reviewsData.df.select("reviewerid", "asin"), productsData.df)
    # joined_df.show(10)
    joined_df.select("reviewerid", "categories").show(20, False)
    # joined_df.write.format("org.apache.spark.sql.cassandra").options(table = "data", keyspace = "amazonreviews").save()





