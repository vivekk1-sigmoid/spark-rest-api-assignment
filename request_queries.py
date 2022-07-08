import json

from flask import Flask, jsonify
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max

app = Flask(__name__)
spark = SparkSession.builder.getOrCreate()
df = spark.read.options(header=True).csv('csv_data')
df.createOrReplaceTempView('stocks')


@app.route('/')
def hello_world():
    return "hello world"


# On each of the days find which stock has moved maximum %age wise in both directions (+ve, -ve)
@app.route('/movement')
def stock_max_movement():
    pass


# 2nd
# Which stock was most traded stock on each day
@app.route("/max_traded_stock")
def most_traded_stock():
    df = spark.sql("select t1.* from `stocks` t1 join ( select Date, Max(Volume) AS max_volume from `stocks` Group By Date) t2 on t1.Date = t2.Date and t1.Volume = t2.max_volume ")
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 3rd
# Which stock had the max gap up or gap down opening
# from the previous day close price I.e. (previous day close - current day open price )
@app.route("/max_min_gap")
def max_min_gap_in_stock_price():
    new_table = spark.sql(" SELECT Date,company,Open,Close , Close - LAG(Open,1,NULL) OVER (PARTITION BY company ORDER BY Date) as gap FROM stocks")
    new_table.createOrReplaceTempView("max_min_table")
    query = "select company, min(gap),max(gap) from max_min_table group by company"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 4th
# Which stock has moved maximum from 1st Day data to the latest day
@app.route("/maximum_movement")
def max_movement_from_first_day_to_last_day():
    query = "select distinct company, abs(first_value(Open) over(partition by company order by Date)- first_value(close) over(partition by company order by Date desc) )as maximum_movement from stocks"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 5th
# Find the standard deviations for each stock over the period
@app.route("/std_deviation")
def standard_deviations_over_the_period():
    query = "SELECT company,stddev(Volume) as Std_deviation_of_stock FROM stocks GROUP BY company"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 6th
# Find the mean and median prices for each stock
@app.route("/mean_and_median")
def mean_and_median_of_stocks():
    query = "SELECT stock_name, percentile_approx(Open, 0.5) as median_open, percentile_approx(Close, 0.5) as median_close, mean(Open) as mean_of_open, mean(Close) as mean_of_close FROM stocks GROUP BY stock_name"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 7th
# Find the average volume over the period
@app.route("/average_of_stock_volume")
def average_of_stock_volume_over_period():
    query = "SELECT stock_name, AVG(Volume) as AvgVolume FROM stocks GROUP BY stock_name"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 8th
# Find which stock has higher average volume
@app.route('/stock_higher_avg_volume')
def stock_higher_avg_volume():
    query = "SELECT stock_name, AVG(Volume) as MaxAvgVolume FROM stocks GROUP BY stock_name ORDER BY AVG(Volume) DESC LIMIT 1"
    pdf = spark.sql(query)
    data = (pdf.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'Data': data})


# 9th
# Find the highest and lowest prices for a stock over the period of time
@app.route("/highest_and_lowest_price")
def stock_highest_and_lowest_price():
    query = "SELECT stock_name, MAX(Open) as MaxStockPrice, MIN(Open) as MinStockPrice FROM stocks group by stock_name"
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


if __name__ == "__main__":
    app.run(debug=True, port=5005)


# sys.path.append('../')

# spark = SparkSession \
#     .builder \
#     .appName("how to read csv file") \
#     .getOrCreate()

# df = spark.read.csv('data/sample_data.csv')

# df = spark.read.csv('csv_data/', header=True, inferSchema = True).drop('_c0')
# df = spark.read.options(header=True).options(inferSchema = True).csv('csv_data')
# df.show()
# df.printSchema()
# df.groupBy("stock_name").count().show()
# df.createOrReplaceTempView("stocks")

# ALTER TABLE df.stocks ALTER COLUMN Open  int;
# ALTER TABLE [] MODIFY COLUMN [Salary] NUMERIC(22,5)




# ques06 = spark.sql("SELECT stock_name, AVG(Open) AS MeanPrice FROM stocks GROUP BY stock_name")
# ques06 = spark.sql("SELECT stock_name, percentile_approx(Open, 0.5) as median_open, percentile_approx(Close, 0.5) as median_close, mean(Open) as mean_of_open, mean(Close) as mean_of_close FROM stocks GROUP BY stock_name")
# ques06.show()

# ques07 = spark.sql("SELECT stock_name, AVG(Volume) as AvgVolume FROM stocks GROUP BY stock_name")
# ques07.show()

# ques08 = spark.sql("SELECT stock_name, AVG(Volume) as MaxAvgVolume FROM stocks GROUP BY stock_name ORDER BY AVG(Volume) DESC LIMIT 1")
# ques08.show()


# ques09 = spark.sql("SELECT stock_name, MAX(Open) as MaxStockPrice, MIN(Open) as MinStockPrice FROM stocks group by stock_name")
# ques09.show()

# sqlDF = spark.sql("ALTER TABLE stocks MODIFY open NUMERIC(22,5)");
# sqlDF.show()
# print(df.printSchema())