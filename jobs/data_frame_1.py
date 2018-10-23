import os

from pyspark.sql.types import StructField, IntegerType, StringType, StructType

from config import DATA_DIR, setup

INPUT = os.path.join(DATA_DIR, 'people.json')

spark = setup("Basics")

df = spark.read.json(INPUT)
df.printSchema()
df.show()
print(df.columns)

df.describe().show()

data_schema = [
    StructField('age', IntegerType(), True),
    StructField('name', StringType(), True),
]

final_struct = StructType(fields=data_schema)
df2 = spark.read.json(INPUT, schema=final_struct)
df2.printSchema()
