#!/usr/bin/python3
import pyspark
import re
import os
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
            userGen.append(line)
except:
    print("el usuario no tiene preferencia por el genero")

userSour=[]
try: 
    with open("userSource.txt") as fileSource:
        for line in fileSource:
            userSour.append(line)
except:
    print("el usuario no tiene preferencia por el source")

userRating=[]
try: 
    with open("userRating.txt") as fileRating:
        for line in fileRating:
            userRating.append(line)
except:
    print("el usuario no tiene preferencia por la calificacion de edad")

userStudio=[]
try: 
    with open("userStudio.txt") as fileStudio:
        for line in fileStudio:
            userStudio.append(line)
except:
    print("el usuario no tiene preferencia por el estudio")

#---------------------------------------------------------------
once=1

filter_size=2

name_array=[""]*filter_size
cont_array=[0]*filter_size

index=0
rango=df_names.count()
#-----------------------------------------------------
#cojo la tabla con el usuario con nombre [index]
for index in range(100):
    name=(df_names.collect()[index])
    print(name)
    user=name.__getitem__('username')
    print(user)
    df_user_aux=dfclean.filter(dfclean.username==user)

    genre_count=0
    studio_count=0
    source_count=0
    rating_count=0
    final_count=0

    end=1
    #generos

    if userGen:
        for genre in userGen:
             df_user_aux=df_user_aux.filter(array_contains(split(df_user_aux.Genres,", "),genre)==True)

    # if genero1!="":
    #     df_user_aux=df_user_aux.filter(array_contains(split(df_user_aux.Genres,", "),genero1)==True)

    # if genero2!="":
    #     df_user_aux=df_user_aux.filter(array_contains(split(df_user_aux.Genres,", "),genero2)==True)

    # #if(genero3 y 4)
    genre_count=df_user_aux.count()

    #source
    if userSour:
        for source in userSour:
            df_user_aux=df_user_aux.filter(df_user_aux.Source==source)

    source_count=df_user_aux.count()

    #studio
    if userStudio:
        for studio in userStudio:
            df_user_aux=df_user_aux.filter(df_user_aux.Studios==studio)

        studio_count=df_user_aux.count()

    #ratings
    if userRating:
        for rating in userRating:
            df_user_aux=df_user_aux.filter(df_user_aux.Rating==rating)
        rating_count=df_user_aux.count()
        
    final_count=df_user_aux.count() 
    
    # name_array.append(user)
    # cont_array.append(final_count)
    # 8 7 6 5 4 
    for i in range(len(name_array)):
        if final_count>cont_array[i] and end==1:
            
           
            j=(len(name_array)-1)
            while j > 0:
                cont_array[j]=cont_array[j-1]
                name_array[j]=name_array[j-1]
                j-=1
            cont_array[i]=final_count
            name_array[i]=user
            end=0


for x in range(len(name_array)):
     print("Contador de "+name_array[x]+":  "+str(cont_array[x]))
    
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
    
