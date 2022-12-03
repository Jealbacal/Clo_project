#!/usr/bin/python3
from email.header import Header
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

conf=SparkConf().setAppName('ProjectAnimeFilter')
sc= SparkContext(conf=conf)
spark=SparkSession.builder.appName('PySpark Read CSV').getOrCreate()

df=spark.read.csv('clean.csv',sep=',',mode="DROPMALFORMED",header=True)
df1=spark.read.csv('AnimeList.csv',sep=',',mode="DROPMALFORMED",header=True)

dfJoin=df.join(df1,df.anime_id==  df1.MAL_ID,"inner")

dfclean=dfJoin.drop("MAL_ID")
dfclean.show()



# df.show()
# df1.show()