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
# Code can be improvised and can be more concise
# Can also use window function
@app.route('/movement')
def stock_max_movement():
    query1 = "select High, Low, Volume, Date, stock_name, ((Close - Open)/Close)*100 as Percent from stocks"
    pdf = spark.sql(query1)

    pdf.createOrReplaceTempView("new_table")

    query2 = "SELECT mt.stock_name as min_stock, mt.Date, mt.Percent as minPerc FROM new_table mt INNER JOIN (SELECT Date, MIN(Percent) AS MinPercent FROM new_table GROUP BY Date) t ON mt.Date = t.Date AND mt.Percent = t.MinPercent"

    query3 = "SELECT mt.stock_name as max_stock, mt.Date, mt.Percent as maxPerc FROM new_table mt INNER JOIN (SELECT Date, MAX(Percent) AS MinPercent FROM new_table GROUP BY Date) t ON mt.Date = t.Date AND mt.Percent = t.MinPercent"

    new_pdf = spark.sql(query2)
    new_pdf.createOrReplaceTempView("t1")

    new_pdf1 = spark.sql(query3)
    new_pdf1.createOrReplaceTempView("t2")
    query4 = "select t1.Date,t1.min_stock,t1.minPerc,t2.max_stock,t2.maxPerc from t1 join t2 on t1.Date = t2.Date"
    df = spark.sql(query4)
    return jsonify(json.loads(df.toPandas().to_json(orient="table",index=False)))

# 2nd
# Which stock was most traded stock on each day
@app.route("/max_traded_stock")
def most_traded_stock():
    df = spark.sql("select t1.stock_name, t1.Volume, t1.Date from `stocks` t1 join ( select Date, Max(Volume) AS max_volume from `stocks` Group By Date) t2 on t1.Date = t2.Date and t1.Volume = t2.max_volume ")
    # data = df.select('*').rdd.flatMap(lambda x: x).collect()
    # return jsonify({'Data': data})
    return jsonify(json.loads(df.toPandas().to_json(orient="table", index=False)))


# 3rd
# Which stock had the max gap up or gap down opening
# from the previous day close price I.e. (previous day close - current day open price )
@app.route("/max_min_gap")
def max_min_gap_in_stock_price():
    # new_table = spark.sql(" SELECT Date,company,Open,Close , Close - LAG(Open,1,NULL) OVER (PARTITION BY company ORDER BY Date) as gap FROM stocks")
    # new_table.createOrReplaceTempView("max_min_table")
    query = "SELECT stock_name, Date, Open, Close , Close- LAG(Open, 1, null) OVER (PARTITION BY stock_name ORDER BY Date) " \
            "as diff FROM stocks "
    df = spark.sql(query)
    data = df.select('*').rdd.flatMap(lambda x: x).collect()
    return jsonify({'Data': data})


# 4th
# Which stock has moved maximum from 1st Day data to the latest day
# Can also use temp view to create temporary view of table
@app.route("/maximum_movement")
def max_movement_from_first_day_to_last_day():
    query = "select distinct stock_name, abs((first_value(Open) over (partition by stock_name order by Date asc) - first_value(Close) over (partition by stock_name order by Date desc))) as diff from stocks "
    pdf = spark.sql(query)
    pdf.createOrReplaceTempView("max_table")
    query2 = "select stock_name, diff from max_table order by diff desc limit(1)"
    new_pdf = spark.sql(query2)
    df = spark.sql(new_pdf)
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
