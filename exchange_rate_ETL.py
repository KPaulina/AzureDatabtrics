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

# Create PySpark DataFrame from Pandas
sparkDF=spark.createDataFrame(df) 
# sparkDF['time_last_update_utc'] = sparkDF['time_last_update_utc'].str[:16]
sparkDF=sparkDF.toDF(*[c.lower().replace('.', '_') for c in df.columns])
sparkDF.printSchema()
sparkDF.show()
display(sparkDF)
#connect to sql database in Azure
sparkDF.write.mode("append") \
.format("jdbc")\
.option("url", "jdbc:sqlserver://exchange-rate-paulina.database.windows.net:1433;databaseName=exchange-rate")\
.option("dbtable", "dbo.exchange_rate_table")\
.option("user", "paulina")\
.option("password", "Cipjbfic12#")\
.save()
