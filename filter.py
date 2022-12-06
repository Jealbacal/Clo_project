#!/usr/bin/python3
import pyspark
import re
from email.header import Header
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import expr
from pyspark.sql.types import FloatType, StringType, StructField, StructType, IntegerType
from pyspark.sql.functions import array_contains,split,array

#import spark.sqlContext.implicits._

conf=SparkConf().setAppName('ProjectAnimeFilter')
sc= SparkContext(conf=conf)
spark=SparkSession.builder.appName('PySpark Read CSV').getOrCreate()

df=spark.read.csv('clean.csv',sep=',',mode="DROPMALFORMED",header=True)
df1=spark.read.csv('AnimeList.csv',sep=',',mode="DROPMALFORMED",header=True)
df2=spark.read.csv('UserList.csv',sep=',',mode="DROPMALFORMED",header=True)

dfJoin=df.join(df1,df.anime_id==df1.MAL_ID,"inner")

dfclean=dfJoin.drop("MAL_ID","Japanese Name","Premiered","Producers","Licensors","Duration","Popularity","Members","Favorites","Watching","Completed"
                    "On-Hold","Dropped","Plan to watch",
                    "Score-10","Score-9","Score-8","Score-7","Score-6","Score-5","Score-4","Score-3","Score-2","Score-1")
#dfclean.show()

df_names=df2.select("username")
df_names.show()
#Row(username='RedvelvetDaisuki')

index=1
name=(df_names.collect()[index])
print(name)
user=name.__getitem__('username')
print(user)

#Genres:  ['Action']
#Sources:  ['Manga']
#Rating:  ['PG-13 - Teens 13 or older']
#Estudio:  mappa
#Popular:  si

genre_count=0
studio_count=0
source_count=0

genero1="Action"
genero2="Adventure"
genero3=""
genero4=""
#puedo hacer un array pero ya otro dia
source="Manga"
studio="Bones"

df_user_aux=dfclean.filter(dfclean.username==user)
df_user_aux.show()

df_user_aux=df_user_aux.filter(array_contains(split(df_user_aux.Genres,", "),genero1)==True)
df_user_aux.show(truncate=False)
df_user_aux=df_user_aux.filter(array_contains(split(df_user_aux.Genres,", "),genero2)==True)
genre_count=df_user_aux.count()

df_user_aux.show(truncate=False)

df_user_aux=df_user_aux.filter(df_user_aux.Source==source)
source_count=df_user_aux.count()
df_user_aux.show(truncate=False)

df_user_aux=df_user_aux.filter(df_user_aux.Studios==studio)
studio_count=df_user_aux.count()
df_user_aux.show(truncate=False)


print("Genero:"+ genero1 +","+ genero2 + " Count: " + str(genre_count))
print("Source:"+ source + " Count: " + str(source_count))
print("Studio:"+ studio + " Count: " + str(studio_count))



#print("Genero:"+ genero + " Count: " + genre_count)
#print("Studio:"+ studio + " Count: " + studio_count)
#dfclean.write.csv("carpeta_datos",header=True)
#df.filter(array_contains(split(df.Genres,","),"Action")==True)#.show()      
#df.select("MAL_ID","Name","Genres","Score","Studios","Type").show()


#-------------------------------------
#df_genre=df_user_aux.filter(array_contains(df_user_aux.Genres,genero)==True).show()
#df_genre=df_user_aux.select("anime_id","Name",(split(df_user_aux.Genres,", ").alias("Genre_Array")))
#df_genre.show(truncate=False)
#df_genre_filtered=df_genre.filter(array_contains(df_genre.Genre_Array,genero)==True)
#df_genre_filtered=df_genre.filter(array_contains(df_genre.Genre_Array,"Mecha")==True)
#df_genre_filtered.show(truncate=False)      

#df.filter(array_contains(split(df.Genres,","),"Action")==True)#.show() 
    
