import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType

from textblob import TextBlob
import datetime

from util import get_s3_path

BUCKET = "amazon-data-insight"


def flat(input_list):
    result = []
    if input_list is None:
        return result
    for sublist in input_list:
        if sublist is not None:
            for val in sublist:
                if val is not None and val not in result:
                    result.append(val)
    return result

class ProductData:
    def __init__(self, path, conf, sc):
        sqlContext = SQLContext(sc)
        self.df = sqlContext.read.format('json').\
            options(header='true', inferSchema='true').\
            load(path)

    def main(self):
        self.df.select("categories").show(10, False)

        flat_udf = functions.udf(flat, ArrayType(StringType()))

        self.df = self.df.withColumn("categories", flat_udf(self.df.categories))
        # self.df.select("categories").show(10, False)

        self.df = self.df.withColumn("related", flat_udf(self.df.related))
        # self.df.select("related").show(10, False)



if __name__ == "__main__":
    conf = SparkConf().setAppName("test")
    sc = SparkContext(conf = conf)

    products_path = get_s3_path(BUCKET, 'product', 'meta_Toys_and_Games.json')
    # products_path = get_s3_path(BUCKET, "product", "metadata.json")

    productsData = ProductData(products_path, conf, sc)
    productsData.main()
