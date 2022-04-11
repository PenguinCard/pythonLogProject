import os
import re

# spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row

# 스파크 세션 생성
spark = SparkSession.builder.config('spark.driver.host', '127.0.0.1').getOrCreate()

rows = []
file_list = os.listdir()
log_file_list = list(filter(lambda s: re.search(r'log', s), file_list))

spark.read.text('*.log').createOrReplaceTempView("LOGS")
df = spark.sql("SELECT * FROM LOGS WHERE value like '%상태 변경%=>%'")

Scheme = StructType([
    StructField("change_date", StringType(), True),
    StructField("change_time", StringType(), True),
    StructField("change_user", StringType(), True),
    StructField("order_item_code", StringType(), True),
    StructField("before_stat", StringType(), True),
    StructField("after_stat", StringType(), True)
])

for row in df.rdd.collect():
    text = re.split(r'\s+', str(row.value).strip())

    if text[7] == '=>':
        text = text[0:7] + ["???"] + text[7:len(text)]

    print(text)

    text = list(filter(lambda s: len(s) > 1, text))
    text = list(filter(lambda s: re.search(r'상태|변경|=>', s) is None, text))
    print(text)

    date = text[0]
    time = ":".join(text[1].split(':'))
    user = text[2]
    order_item_code = text[3]
    stat_past = text[4]
    stat_now = text[5]

    rows.append(
        Row(
            date,
            time,
            user,
            order_item_code,
            stat_past,
            stat_now
        )
    )

spark.createDataFrame(rows, Scheme).createOrReplaceTempView("CHANGE_STAT")

rst = spark.sql("""
    SELECT * 
      FROM CHANGE_STAT
     WHERE order_item_code = '20220219-0000771-03'
""")

rst.show()



# print(log_file_list)
#
# def file_log_to_txt(file_name):
#
#     new_file_name = re.sub(r'\.log', '.txt', file_name)
#
#     with open(new_file_name, 'w') as f:
#         f.write(open(file_name).read())
#
#
# print(log_file_list[0])
#
# file_log_to_txt(log_file_list[0])