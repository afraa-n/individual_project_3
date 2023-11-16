"""
transform and load function
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/individual_project_3/iris.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    iris_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    iris_df = iris_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it 
    iris_df.write.format("delta").mode("overwrite").saveAsTable("iris_delta")
    
    num_rows = iris_df.count()
    print(num_rows)
    
    return "finished transform and load"

if __name__ == "__main__":
    load()