import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions
from pyspark.sql.types import FloatType, ArrayType, StringType

from textblob import TextBlob
import datetime

BUCKET = "amazon-data-insight"

def get_s3_path(bucket_name, folder_name=None, file_name=None):
    path = 's3a://{}/'.format(bucket_name)

    if folder_name is not None:
        path += '{}/'.format(folder_name)
    if file_name is not None:
        path += '{}/'.format(file_name)

    return path
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