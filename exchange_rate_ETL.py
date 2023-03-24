# Databricks notebook source
import requests
import json
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

res = requests.request("GET", 'https://open.er-api.com/v6/latest/PLN')
json_data = res.json()
json_object = json.dumps(json_data, indent=4)
jason_data = json.loads(json_object)

# COMMAND ----------

pd.set_option('display.max_columns', None)
df = pd.json_normalize(jason_data, max_level=1)
list_of_columns = df.columns.to_list()
df = df[['time_last_update_utc', 'rates.PLN', 'rates.AED', 'rates.AFN', 'rates.ALL', 'rates.AMD', 'rates.ANG', 'rates.AOA', 'rates.ARS', 'rates.AUD', 'rates.AWG', 'rates.AZN', 'rates.BAM', 'rates.BBD', 'rates.BDT', 'rates.BGN', 'rates.BHD', 'rates.BIF', 'rates.BMD', 'rates.BND', 'rates.BOB', 'rates.BRL', 'rates.BSD', 'rates.BTN', 'rates.BWP', 'rates.BYN', 'rates.BZD', 'rates.CAD', 'rates.CDF', 'rates.CHF', 'rates.CLP', 'rates.CNY', 'rates.COP', 'rates.CRC', 'rates.CUP', 'rates.CVE', 'rates.CZK', 'rates.DJF', 'rates.DKK', 'rates.DOP', 'rates.DZD', 'rates.EGP', 'rates.ERN', 'rates.ETB', 'rates.EUR', 'rates.FJD', 'rates.FKP', 'rates.FOK', 'rates.GBP', 'rates.GEL', 'rates.GGP', 'rates.GHS', 'rates.GIP', 'rates.GMD', 'rates.GNF', 'rates.GTQ', 'rates.GYD', 'rates.HKD', 'rates.HNL', 'rates.HRK', 'rates.HTG', 'rates.HUF', 'rates.IDR', 'rates.ILS', 'rates.IMP', 'rates.INR', 'rates.IQD', 'rates.IRR', 'rates.ISK', 'rates.JEP', 'rates.JMD', 'rates.JOD', 'rates.JPY', 'rates.KES', 'rates.KGS', 'rates.KHR', 'rates.KID', 'rates.KMF', 'rates.KRW', 'rates.KWD', 'rates.KYD', 'rates.KZT', 'rates.LAK', 'rates.LBP', 'rates.LKR', 'rates.LRD', 'rates.LSL', 'rates.LYD', 'rates.MAD', 'rates.MDL', 'rates.MGA', 'rates.MKD', 'rates.MMK', 'rates.MNT', 'rates.MOP', 'rates.MRU', 'rates.MUR', 'rates.MVR', 'rates.MWK', 'rates.MXN', 'rates.MYR', 'rates.MZN', 'rates.NAD', 'rates.NGN', 'rates.NIO', 'rates.NOK', 'rates.NPR', 'rates.NZD', 'rates.OMR', 'rates.PAB', 'rates.PEN', 'rates.PGK', 'rates.PHP', 'rates.PKR', 'rates.PYG', 'rates.QAR', 'rates.RON', 'rates.RSD', 'rates.RUB', 'rates.RWF', 'rates.SAR', 'rates.SBD', 'rates.SCR', 'rates.SDG', 'rates.SEK', 'rates.SGD', 'rates.SHP', 'rates.SLE', 'rates.SLL', 'rates.SOS', 'rates.SRD', 'rates.SSP', 'rates.STN', 'rates.SYP', 'rates.SZL', 'rates.THB', 'rates.TJS', 'rates.TMT', 'rates.TND', 'rates.TOP', 'rates.TRY', 'rates.TTD', 'rates.TVD', 'rates.TWD', 'rates.TZS', 'rates.UAH', 'rates.UGX', 'rates.USD', 'rates.UYU', 'rates.UZS', 'rates.VES', 'rates.VND', 'rates.VUV', 'rates.WST', 'rates.XAF', 'rates.XCD', 'rates.XDR', 'rates.XOF', 'rates.XPF', 'rates.YER', 'rates.ZAR', 'rates.ZMW', 'rates.ZWL']]

# COMMAND ----------

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

jdbcHostname = 'exchange-rate-paulina.database.windows.net'
jdbcPort = 1433
jdbcDatabase = "exchange-rate"
jdbcUsername = "paulina"
jdbcPassword = "test"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

#Create PySpark DataFrame from Pandas
sparkDF=spark.createDataFrame(df) 
sparkDF.printSchema()
sparkDF.show()
display(sparkDF)
# query = f"""INSERT INTO exchange_rate_table VALUES("Mon, 20 Mar 2023", 1.0, 0.8337, 19.8574, 24.3759, 88.0263, 0.4064, 116.0922, 46.0461, 0.3381, 0.4064, 0.3868, 0.4163, 0.454, 24.2823, 0.4163, 0.0854, 470.1641, 0.227, 0.3042, 1.5676, 1.1859, 0.227, 18.6856, 3.0099, 0.5775, 0.454, 0.3111, 464.481, 0.2105, 186.9872, 1.5613, 1098.5594, 123.7419, 5.4485, 23.4725, 5.0971, 40.3463, 1.5881, 12.4242, 30.8656, 6.9852, 3.4053, 12.2167, 0.2129, 0.5019, 0.1866, 1.5881, 0.1867, 0.5815, 0.1866, 2.8281, 0.1866, 14.2871, 1933.9236, 1.7672, 47.8765, 1.7827, 5.5805, 1.6039, 34.9604, 84.5418, 3473.4815, 0.8346, 0.1866, 18.6856, 331.2489, 9530.8879, 31.7748, 0.1866, 34.3796, 0.161, 30.0688, 29.4764, 19.8259, 915.6461, 0.3381, 104.727, 295.2959, 0.0694, 0.1892, 103.8871, 3848.8175, 3405.3074, 76.6608, 36.6269, 4.1585, 1.0943, 2.352, 4.2162, 979.5262, 13.1055, 475.6522, 799.8479, 1.8246, 7.815, 10.5573, 3.4937, 237.4729, 4.286, 1.0152, 14.513, 4.1585, 104.2946, 8.3034, 2.4295, 29.8969, 0.363, 0.0873, 0.227, 0.8596, 0.7982, 12.3399, 63.8816, 1620.8918, 0.8264, 1.0463, 24.9434, 17.4092, 250.9898, 0.8513, 1.9133, 3.0715, 103.931, 2.3831, 0.3042, 0.1866, 4.8614, 4861.4487, 129.0569, 7.946, 182.2848, 5.2154, 570.1547, 4.1585, 7.7735, 2.4693, 0.7924, 0.704, 0.5321, 4.3161, 1.5353, 0.3381, 6.9499, 532.0569, 8.3483, 850.0486, 0.227, 8.9455, 2575.8964, 5.4947, 5352.0144, 26.9342, 0.6173, 139.636, 0.613, 0.1691, 139.636, 25.4027, 56.7302, 4.1646, 4.6739, 208.3333)"""

# df1 = spark.read.format("jdbc").option("url", jdbcUrl).option("query", f"{query}").load()
# display(df1)
