import os

from config import DATA_DIR, setup

INPUT = os.path.join(DATA_DIR, 'appl_stock.csv')


def filter_sql(df):
    return df.filter("Close < 500")


def filter_python(df):
    return df.filter(df['Close'] < 500)


def filter_multiple(df):
    return df.filter((df['Open'] > 200) & (df['Close'] < 200))


def get_by_low_value(df, val):
    return df.filter(df['Low'] == val).collect()


spark = setup("BasicOps")
df = spark.read.csv(INPUT, inferSchema=True, header=True)

filter_sql(df).select(['Open', 'Close']).show()
filter_python(df).select(['Open', 'Close']).show()
filter_multiple(df).select(['Open', 'Close']).show()
result = get_by_low_value(df, 197.16)
row = result[0]
print(row.asDict())
