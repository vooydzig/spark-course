import os
from config import DATA_DIR, setup

from pyspark.sql.functions import mean

INPUT = os.path.join(DATA_DIR, 'ContainsNull.csv')

spark = setup("MissingData")
df = spark.read.csv(INPUT, inferSchema=True, header=True)

df.na.drop().show()  # drops all rows with any missing data
df.na.drop(thresh=2).show()  # drops all rows with less than 2 filled columns
df.na.drop(how='all').show()  # drops all rows with all null columns
df.na.drop(subset=['Sales']).show()  # drops all rows missin 'Sales' value

df.na.fill('FILL VALUE').show()  # sets any null in "string" column to "FILL VALUE"
df.na.fill(0).show()  # sets any null in "numeric" column to "0"
df.na.fill('No Name', subset=['Name']).show()  # sets any null in "Name" column to "No Name"

mean = df.select(mean(df['Sales'])).collect()
mean_sales = mean[0][0]
df.na.fill(mean_sales, subset=['Sales']).show()  # fills empty Sales with mean sales values