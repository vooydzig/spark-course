import os

from pyspark.sql.functions import countDistinct, avg, stddev, format_number

from config import DATA_DIR, setup

INPUT = os.path.join(DATA_DIR, 'sales_info.csv')


def group_by_company(df):
    return df.groupBy("Company")


spark = setup("Grouping")
df = spark.read.csv(INPUT, inferSchema=True, header=True)

group_by_company(df).mean().show()
group_by_company(df).sum().show()
group_by_company(df).max().show()
group_by_company(df).min().show()
group_by_company(df).count().show()

df.agg({'Sales': 'sum'}).show()
df.agg({'Sales': 'min'}).show()
df.agg({'Sales': 'max'}).show()

group_by_company(df).agg({'Sales': 'max'}).show()

df.select(countDistinct('Sales')).show()

df.select(avg('Sales')).show()
df.select(avg('Sales').alias('Average sales')).show()

df.select(stddev('Sales')).show()

df.select(stddev('Sales').alias('std')) \
    .select(format_number('std', 2).alias('std')).show()

df.orderBy('Sales').show()  # ascending order
df.orderBy(df['Sales'].desc()).show()  # descending order
