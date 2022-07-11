import json

from flask import Flask, jsonify
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max

app = Flask(__name__)
spark = SparkSession.builder.getOrCreate()
df = spark.read.options(header=True, inferSchema = True).csv('csv_data').drop('_c0')
df.createOrReplaceTempView('stocks')



# t1 = spark.sql("select distinct stock_name, abs((first_value(Open) over (partition by stock_name order by Date asc) - first_value(" \
#             "Close) over (partition by stock_name order by Date desc))) as diff from stocks")

# query = "select distinct stock_name, abs((first_value(Open) over (partition by stock_name order by Date asc) - first_value(" \
#             "Close) over (partition by stock_name order by Date desc))) as diff from stocks "
# pdf = spark.sql(query)
# pdf.createOrReplaceTempView("max_table")
# query2 = "select stock_name, diff from max_table order by diff desc limit(1)"
# new_pdf = spark.sql(query2)
# new_pdf.show()


ques07 = spark.sql("select distinct company, abs(first_value(Open) over(partition by company order by Date)- first_value(close) over(partition by company order by Date desc) )as maximum_movement from stocks")
ques07.show()



# def get_max_moved_stocks_per_day(spark, df):
#     df.createOrReplaceTempView('stocks')
#     return spark.sql('CREATE TEMPORARY VIEW query AS (SELECT Date, Stock, UpChange, DownChange, RANK() OVER (PARTITION BY Date ORDER BY UpChange DESC) AS MaxUpRank, RANK() OVER (PARTITION BY Date ORDER BY DownChange) AS MaxDownRank FROM (SELECT Stock, Date, ((High - Open)/Open) AS UpChange, ((Low - Open)/Open) AS DownChange FROM stocks))')
#
#
# def get_most_traded_stock_per_day(spark, df):
#     df.createOrReplaceTempView('stocks')
#     return spark.sql('SELECT Date, Stock, Volume, RANK() OVER (PARTITION BY Date ORDER BY Volume DESC) AS MostTradedRank FROM stocks')
#
#
# def get_max_gap_per_stock_per_day(spark, df):
#     df.createOrReplaceTempView('stocks')
#     return spark.sql('SELECT T3.Stock AS Stock, T3.Date AS Cur_Date, T4.Date AS Prev_Date, T3.Open AS Cur_Open, T4.Close AS Prev_Close, (T3.Open - T4.Close) AS Gap, RANK() OVER (PARTITION BY T3.Date ORDER BY (T3.Open - T4.Close) DESC) AS GapRank FROM (SELECT T1.Date AS Cur_Date, MAX(T2.Date) AS Prev_Date FROM stocks AS T1 INNER JOIN stocks AS T2 WHERE T1.Date > T2.Date GROUP BY Cur_Date ORDER BY Cur_Date) AS subTable INNER JOIN stocks AS T3 ON subTable.Cur_Date = T3.Date INNER JOIN stocks AS T4 ON subTable.Prev_Date = T4.Date WHERE T3.Stock = T4.Stock').show()
#
#
# def get_most_moved_stock(spark, df):
#     df.createOrReplaceTempView('stocks')
#     return spark.sql('SELECT T1.Stock, min_date, max_date, (T2.Close - T1.Close) AS StockMoved, ROW_NUMBER() OVER (ORDER BY (T2.Close - T1.Close) DESC) AS MovementRank FROM (SELECT MIN(DATE) AS min_date, MAX(DATE) AS max_date FROM stocks) AS Subtable INNER JOIN stocks AS T1 ON Subtable.min_date = T1.Date INNER JOIN stocks AS T2 ON Subtable.max_date = T2.Date WHERE T1.Stock = T2.Stock').show(




