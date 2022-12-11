#!/usr/bin/python3
import pyspark
import re
import os
from email.header import Header
from pyspark import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, StringType, StructField, StructType, IntegerType
from pyspark.sql.functions import array_contains,split,array


#import spark.sqlContext.implicits._

conf=SparkConf().setAppName('ProjectAnimeFilter')
sc= SparkContext(conf=conf)
spark=SparkSession.builder.appName('PySpark Read CSV').getOrCreate()

df=spark.read.csv('clean.csv',sep=',',mode="DROPMALFORMED",header=True)
df1=spark.read.csv('Animes.csv',sep=',',mode="DROPMALFORMED",header=True)
df2=spark.read.csv('UserList.csv',sep=',',mode="DROPMALFORMED",header=True)

dfJoin=df.join(df1,df.anime_id==df1.MAL_ID,"inner")

dfclean=dfJoin.drop("MAL_ID")
#dfclean.show()
df_names=df2.select("username")
#Row(username='RedvelvetDaisuki')

#Genres:  ['Action']
#Sources:  ['Manga']
#Rating:  ['PG-13 - Teens 13 or older']
#Estudio:  mappa
#Popular:  si


# genero1="Action"
# genero2="Adventure"
# genero3=""
# genero4=""
# #puedo hacer un array pero ya otro dia
# source="Manga"
# studio="Bones"
# rating=""
#------------------------------------------------------------------
#Recojida de datos seleccionados por el usuario
userGen=[]
try: 
    with open("userGeneros.txt") as fileGen:
         for line in fileGen:
             line=line.strip("\n")
             userGen.append(line)
     
except:
    print("el usuario no tiene preferencia por el genero")

userSour=[]
try: 
    with open("userSource.txt") as fileSource:
        for line in fileSource:
            line=line.strip("\n")
            userSour.append(line)
except:
    print("el usuario no tiene preferencia por el source")

userRating=[]
try: 
    with open("userRating.txt") as fileRating:
        for line in fileRating:
            line=line.strip("\n")
            userRating.append(line)
except:
    print("el usuario no tiene preferencia por la calificacion de edad")

userStudio=[]
try: 
    with open("userStudio.txt") as fileStudio:
        for line in fileStudio:
            line=line.strip("\n")
            userStudio.append(line)
except:
    print("el usuario no tiene preferencia por el estudio")

#---------------------------------------------------------------
once=1

filter_size=3

name_array=[""]*filter_size
cont_array=[0]*filter_size

index=0
rango=df_names.count()
#-----------------------------------------------------
#cojo la tabla con el usuario con nombre [index]
# for index in range(5):
#     name=(df_names.collect()[index])
#     print(name)
#     user=name.__getitem__('username')
#     print(user)
#     df_user_aux=dfclean.filter(dfclean.username==user)

#     genre_count=0
#     studio_count=0
#     source_count=0
#     rating_count=0
#     final_count=0

#     end=1
#     #generos

#     if userGen:
#         for genre in userGen:
#              df_user_aux=df_user_aux.filter(array_contains(split(df_user_aux.Genres,", "),genre)==True)

#     genre_count=df_user_aux.count()

#     #source
#     if userSour:
#         for source in userSour:
#             df_user_aux=df_user_aux.filter(df_user_aux.Source==source)
#             #df_user_aux.show()
             
#     source_count=df_user_aux.count()

#     #studio
#     if userStudio:
#         for studio in userStudio:
#             df_user_aux=df_user_aux.filter(df_user_aux.Studios==studio)
#             #df_user_aux.show()
             
#         studio_count=df_user_aux.count()

#     #ratings
#     if userRating:
#         for rating in userRating:
#             df_user_aux=df_user_aux.filter(df_user_aux.Rating==rating)
#             #df_user_aux.show()
            
#         rating_count=df_user_aux.count()
   
#     final_count=df_user_aux.count() 
 
   
#     for i in range(len(name_array)):
#         if final_count>=cont_array[i] and end==1:
            
           
#             j=(len(name_array)-1)
#             while j > 0:
#                 cont_array[j]=cont_array[j-1]
#                 name_array[j]=name_array[j-1]
#                 j-=1
#             cont_array[i]=final_count
#             name_array[i]=user
#             end=0

#df_user_aux.show()

######################################
list_genero=dfclean.select('username','anime_id','Genres').filter(col('Source') == userSour[0])\
    .groupBy("username").count().rdd.map(lambda row:(row[0],row[1])).collect()

with open(r'sourceUserV34.csv', 'w') as fp:
    fp.write('username,count')
    fp.write('\n')
    for item in list_genero:
        # write each item on a new line
        for x in item:
            fp.write(str(x)+',')
        fp.write('\n')

dfcount=spark.read.csv('sourceUserV34.csv',sep=',',mode="DROPMALFORMED",header=True)
listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
result=[t[0] for t in list_genero if t[1]==int(listMax[0])]

######################################

sourceRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('Source')==userSour[0])&(col('my_score')>=9)).limit(5).rdd.map(lambda row:row[1]).collect()

with open(r'SourceRecomendation.txt', 'w') as fp:
    for item in sourceRecomendation:
        # write each item on a new line
        fp.write("%s\n" % item)


# # for x in range(len(name_array)):
# #      print("Contador de "+name_array[x]+":  "+str(cont_array[x]))
     
# # print("asduihygtaush" + str(userRating)+"asnduiuasnd"+str(userStudio)+"asndubhasidjs"+str(userSour)+"asdnasind"+str(userGen))
# # print("rango=:"+str(rango))
# print("Genero:"+ genero1 +","+ genero2 + " Count: " + str(genre_count))
# print("Source:"+ source + " Count: " + str(source_count))
# print("Studio:"+ studio + " Count: " + str(studio_count))# contador final de cuantos animes filtrados con todos los parametros hay



#--------------------------------------------------



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