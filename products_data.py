import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType

from textblob import TextBlob
import datetime
from sets import Set

BUCKET = "amazon-data-insight"

def get_s3_path(bucket_name, folder_name=None, file_name=None):
    path = 's3a://{}/'.format(bucket_name)

    if folder_name is not None:
        path += '{}/'.format(folder_name)
    if file_name is not None:
        path += '{}/'.format(file_name)

    return path
def flat(cat):
    result = Set()
    if cat is None:
        return list(result)
    for c in cat:
        if c is not None:
            result.add(c)

    return list(result)

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



if __name__ == "__main__":
    conf = SparkConf().setAppName("test")
    sc = SparkContext(conf = conf)

    products_path = get_s3_path(BUCKET, 'product', 'meta_Toys_and_Games.json')
    # products_path = get_s3_path(BUCKET, "product", "metadata.json")

    productsData = ProductData(products_path, conf, sc)
    productsData.main()
