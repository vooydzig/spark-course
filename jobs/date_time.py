import os

from pyspark.sql.functions import dayofmonth, hour, dayofyear, month, year, weekofyear, format_number

from config import DATA_DIR, setup

INPUT = os.path.join(DATA_DIR, 'appl_stock.csv')

spark = setup("Dates")
df = spark.read.csv(INPUT, inferSchema=True, header=True)

df.select(['Date', 'Open']).show()

df.select([dayofmonth(df['Date']), 'Date']).show()
df.select([hour(df['Date']), 'Date']).show()
df.select([dayofyear(df['Date']), 'Date']).show()
df.select([month(df['Date']), 'Date']).show()
df.select([year(df['Date']), 'Date']).show()
df.select([weekofyear(df['Date']), 'Date']).show()

newdf = df.withColumn('Year', year(df['Date']))
result = newdf.groupBy('Year').mean().select(['Year', "avg(Close)"])
result.select(['Year', format_number("avg(Close)", 2).alias("Average Closing Price")]).show()
