import requests
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType
from pyspark.sql.functions import *

class Tender:
    def __init__(self, spark_context):
        self.session = requests.Session()
        self.url = "https://tenders.guru/api/pl/tenders"
        self.spark_context = spark_context
        self.schema = StructType([
            StructField('title', StringType(), True),
            StructField('id', IntegerType(), True),
            StructField('type', StringType(), True),
            StructField('load_date', DateType(), True)
            ])
        self.data_frame = spark_context.createDataFrame([],self.schema)

    def get_tenders(self, max_pages=-1):
        first_page = self.session.get(self.url).json()
        yield first_page
        if max_pages == -1:
            num_pages = first_page['total']
        else:
            num_pages = max_pages
        for page in range(2, num_pages + 1):
            next_page = self.session.get(self.url, params={'page': page}).json()
            yield next_page
    
    def transform_data(self,max_pages):
        for x in self.get_tenders(max_pages=max_pages): 
            data_frame = self.data_frame\
                .union(self.spark_context\
                .read.option("multiline", "true")\
                .json(self.spark_context.sparkContext.parallelize([json.dumps(x)]))\
                .withColumn("title", explode("data.title"))
                .withColumn("id", explode("data.id"))
                .withColumn("type", explode("data.type.name"))
                .withColumn("type", explode("data.type.name"))
                .withColumn("load_date",current_date())
                .select("title","id","type","load_date")
                .dropDuplicates()       
            )
        return data_frame