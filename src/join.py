import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType, IntegerType
import psycopg2
import os

from util import get_s3_path
from reviews_data import ReviewsData
from products_data import ProductData

CATEGORIES = {'books': 'Books', 
              'electronics': 'Electronics', 
              'moviestv': 'Movies_and_TV', 
              'cdsvinyl': 'CDs_and_Vinyl', 
              'clothingshoesjewelry': 'Clothing_Shoes_and_Jewelry', 
              'homekitchen': 'Home_and_Kitchen', 
              'kindlestore': 'Kindle_Store', 
              'sportsoutdoors': 'Sports_and_Outdoors', 
              'cellphonesaccessories': 'Cell_Phones_and_Accessories', 
              'healthpersonalcare': 'Health_and_Personal_Care', 
              'toysgames': 'Toys_and_Games', 
              'videogames': 'Video_Games', 
              'toolshomeimprovement': 'Tools_and_Home_Improvement', 
              'beauty': 'Beauty', 
              'appsforandroid': 'Apps_for_Android', 
              'officeproducts': 'Office_Products', 
              'petsupplies': 'Pet_Supplies', 
              'automotive': 'Automotive', 
              'grocerygourmetfood': 'Grocery_and_Gourmet_Food', 
              'patiolawngarden': 'Patio_Lawn_and_Garden', 
              'baby': 'Baby', 
              'digitalmusic': 'Digital_Musc', 
              'musicalinstruments': 'Musical_Instruments', 
              'amazoninstantvideo': 'Amazon_Instant_Video'}

def flatAll(input_list):
    result = []
    if input_list is None:
        return result
    for sublist in input_list:
        try:
            if sublist is not None:
                for val in sublist:
                    result.append(val)
        except:
            pass

    return result

def joinDF(rev_df, prod_df, cat):
    postgres_url = os.getenv('POSTGRES_URL', 'default')
    postgres_user = os.getenv('POSTGRES_USER', 'default')
    postgres_pw = os.getenv('POSTGRES_PW', 'default')
    postgres_properties = {
        "user": postgres_user,
        "password": postgres_pw
    }

    table_name = cat+"joined"
    prod_df = prod_df.filter(functions.array_contains("categories", cat))
    joined_df = rev_df.join(prod_df, rev_df.asin == prod_df.productid)
    joined_df = joined_df.groupby("reviewerid").agg(functions.collect_list("categories").alias("categories"))
    flatAll_udf = functions.udf(flatAll, ArrayType(StringType()))
    joined_df = joined_df.withColumn("categories", flatAll_udf(joined_df.categories))
    joined_df = joined_df.rdd.flatMap(lambda (user, cats) : [(user, cat) for cat in cats]).toDF(["reviewerid", "category"])
    joined_df = joined_df.groupby("reviewerid").pivot("category").count()
    joined_df = joined_df.na.fill(0)
    joined_df.write.jdbc(url=postgres_url, table=table_name, mode='overwrite', properties=postgres_properties)
    return


   
if __name__ == "__main__":

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

    postgres_user = os.getenv('POSTGRES_USER', 'default')
    postgres_pw = os.getenv('POSTGRES_PW', 'default')

    conf = SparkConf().setAppName("amazon")\
            .set("spark.driver.maxResultSize", "2g")\
            .set("spark.driver.memory", "3g")
    sc = SparkContext(conf = conf)

    sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', aws_access_key)
    sc._jsc.hadoopConfiguration().set('fs.s3a.awsSecretAccessKey',aws_secret_access_key)

    reviewsData = ReviewsData(c, conf, sc)
    reviewsData.transform()

    productsData = ProductData(c, conf, sc)
    productsData.main()
    for c in CATEGORIES:
        joinDF(reviewsData.df.select("reviewerid", "asin"), productsData.df, c)
