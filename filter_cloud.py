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
from google.cloud import storage

storage_client = storage.Client()
bucket = storage_client.get_bucket("hdswa")

conf=SparkConf().setAppName('ProjectAnimeFilter')
sc= SparkContext(conf=conf)
spark=SparkSession.builder.appName('PySpark Read CSV').getOrCreate()

df=spark.read.csv('gs://hdswa/code/clean.csv',sep=',',mode="DROPMALFORMED",header=True)
df1=spark.read.csv('gs://hdswa/code/animes.csv',sep=',',mode="DROPMALFORMED",header=True)
df2=spark.read.csv('gs://hdswa/code/UserList.csv',sep=',',mode="DROPMALFORMED",header=True)

dfJoin=df.join(df1,df.anime_id==df1.MAL_ID,"inner")

dfclean=dfJoin.drop("MAL_ID")
df_names=df2.select("username")
#------------------------------------------------------------------
#Recojida de datos seleccionados por el usuario
userGen=[]
try: 
    blob=bucket.blob("code/userGeneros.txt")
    blob=blob.download_as_string()
    blob=blob.decode("utf-8")
    #print("blob after utf8:"+str(blob))
    string=str(blob)
    userGen=string.split("\n")
    userGen.pop()
     
except:
    print("el usuario no tiene preferencia por el genero")

userSour=[]
try: 
    blob=bucket.blob("code/userSource.txt")
    blob=blob.download_as_string()
    blob=blob.decode("utf-8")
    #print("blob after utf8:"+str(blob))
    string=str(blob)
    userSour=string.split("\n")
    userSour.pop()
except:
    print("el usuario no tiene preferencia por el source")

userRating=[]
try: 
    blob=bucket.blob("code/userRating.txt")
    blob=blob.download_as_string()
    blob=blob.decode("utf-8")
    #print("blob after utf8:"+str(blob))
    string=str(blob)
    userRating=string.split("\n")
    userRating.pop()
except:
    print("el usuario no tiene preferencia por la calificacion de edad")

userStudio=[]
try: 
    blob=bucket.blob("code/userStudio.txt")
    blob=blob.download_as_string()
    blob=blob.decode("utf-8")
    #print("blob after utf8:"+str(blob))
    string=str(blob)
    userStudio=string.split("\n")
    userStudio.pop()
except:
    print("el usuario no tiene preferencia por el estudio")

def cmpTupla(tupla):
    return(tupla[1])


#Recomendacion de animes con todos los filtros aplicados ( escogidos por el usuario)
######################################
list_All=dfclean.select('username','anime_id','Genres').filter((col('Source').isin(userSour)) & (col('Studios')==userStudio[0]) )\
    .groupBy("username").count().rdd.map(lambda row:(row[0],row[1])).collect()

with open(r'UserRecomendation.csv', 'w') as fp:
    fp.write('username,count')
    fp.write('\n')
    for item in list_All:
        # write each item on a new line
        for x in item:
            fp.write(str(x)+',')
        fp.write('\n')
        
local_path=os.path.abspath('UserRecomendation.csv')
blob=bucket.blob("code/UserRecomendation.csv")
blob.upload_from_filename(local_path)

dfcount=spark.read.csv('gs://hdswa/code/UserRecomendation.csv',sep=',',mode="DROPMALFORMED",header=True)
listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
result=[t[0] for t in list_All if t[1]==int(listMax[0])]


userRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('Source').isin(userSour)) & (col('Studios')==userStudio[0]) & (col('my_score')>=5)).limit(5).rdd.map(lambda row:row[1]).collect()

with open(r'UserRecomendation.txt', 'w') as fp:
    for item in userRecomendation:
        # write each item on a new line
        fp.write("%s\n" % item)

print("Fin de UserRecomendation")
# #Recomendacion de Studio
#########################################################################
if userStudio:
    list_Studio=dfclean.select('username','anime_id','Genres').filter((col('Studios') == userStudio[0]) )\
        .groupBy("username").count().rdd.map(lambda row:(row[0],row[1])).collect()

    with open(r'studioUser.csv', 'w') as fp:
        fp.write('username,count')
        fp.write('\n')
        for item in list_Studio:
            # write each item on a new line
            for x in item:
                fp.write(str(x)+',')
            fp.write('\n')
            
    local_path=os.path.abspath('studioUser.csv')
    blob=bucket.blob("code/studioUser.csv")
    blob.upload_from_filename(local_path)
    
    dfcount=spark.read.csv('gs://hdswa/code/studioUser.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_Studio if t[1]==int(listMax[0])]


    studioRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('Studios')==userStudio[0])).orderBy(col('my_score').desc())\
        .limit(5).rdd.map(lambda row:row[1]).collect()

    with open(r'StudioRecomendation.txt', 'w') as fp:
        for item in studioRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)
print("Fin de StudioRecomendation")
# #Recomendacion por Source
#########################################################################
if userSour:
    list_Source=dfclean.select('username','anime_id','Genres').filter((col('Source') == userSour[0]) )\
        .groupBy("username").count().rdd.map(lambda row:(row[0],row[1])).collect()

    with open(r'sourceUser.csv', 'w') as fp:
        fp.write('username,count')
        fp.write('\n')
        for item in list_Source:
            # write each item on a new line
            for x in item:
                fp.write(str(x)+',')
            fp.write('\n')
            
    local_path=os.path.abspath('sourceUser.csv')
    blob=bucket.blob("code/sourceUser.csv")
    blob.upload_from_filename(local_path)
    
    dfcount=spark.read.csv('gs://hdswa/code/sourceUser.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_Source if t[1]==int(listMax[0])]


    sourceRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('Source').isin(userSour))).orderBy(col('my_score').desc())\
        .limit(5).rdd.map(lambda row:row[1]).collect()

    with open(r'SourceRecomendation.txt', 'w') as fp:
        for item in sourceRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)
print("Fin de SourceRecomendation")
# #Recomendacion por Rating
# #########################################################################
if userRating:

    list_Rating=dfclean.select('username','anime_id','Genres').filter((col('Rating').isin(userRating)))\
        .groupBy("username").count().rdd.map(lambda row:(row[0],row[1])).collect()

    with open(r'ratingUser.csv', 'w') as fp:
        fp.write('username,count')
        fp.write('\n')
        for item in list_Rating:
            # write each item on a new line
            for x in item:
                fp.write(str(x)+',')
            fp.write('\n')

    local_path=os.path.abspath('ratingUser.csv')
    blob=bucket.blob("code/ratingUser.csv")
    blob.upload_from_filename(local_path)
    
    dfcount=spark.read.csv('gs://hdswa/code/ratingUser.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_Rating if t[1]==int(listMax[0])]

    ratingRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('rating').isin(userRating))).orderBy(col('my_score').desc())\
        .limit(5).rdd.map(lambda row:row[1]).collect()

    with open(r'RatingRecomendation.txt', 'w') as fp:
        for item in ratingRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)
print("Fin de RatingRecomendation")
#Recomendacion por genre
#########################################################################
if userGen:

    list_Gen=dfclean.select('username','anime_id','Genres').filter(col('Genres').isin(userGen))\
        .groupBy("username").count().rdd.map(lambda row:(row[0],row[1])).collect()

    with open(r'genreUser.csv', 'w') as fp:
        fp.write('username,count')
        fp.write('\n')
        for item in list_Gen:
            # write each item on a new line
            for x in item:
                fp.write(str(x)+',')
            fp.write('\n')

    local_path=os.path.abspath('genreUser.csv')
    blob=bucket.blob("code/genreUser.csv")
    blob.upload_from_filename(local_path)
    
    dfcount=spark.read.csv('gs://hdswa/code/genreUser.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_Gen if t[1]==int(listMax[0])]

    genreRecomendation=dfclean.select('Name','my_score').filter((col('username')==result[0]) & (col('Genres').isin(userGen))).orderBy(col('my_score').desc())\
        .limit(5).rdd.map(lambda row:(row[0])).collect()
        
    with open(r'GenreRecomendation.txt', 'w') as fp:
        for item in genreRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)
print("Fin de GenreRecomendation")
#Recomendacion de animes con todos los filtros aplicados ( escogidos por el usuario)
######################################
userRecomendation=[]
if userStudio:
    list_All=dfclean.select('username','anime_id','Genres').filter((col('Source').isin(userSour)) & (col('Studios')==userStudio[0]) & (col('Genres').isin(userGen)) & (col('Rating').isin(userRating)))\
            .groupBy("username").count().rdd.map(lambda row:(row[0],row[1])).collect()

    with open(r'UserRecomendation.csv', 'w') as fp:
            fp.write('username,count')
            fp.write('\n')
            for item in list_All:
                # write each item on a new line
                for x in item:
                    fp.write(str(x)+',')
                fp.write('\n')
    
    local_path=os.path.abspath('UserRecomendation.csv')
    blob=bucket.blob("code/UserRecomendation.csv")
    blob.upload_from_filename(local_path)

    dfcount=spark.read.csv('gs://hdswa/code/UserRecomendation.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_All if t[1]==int(listMax[0])]
    if result:
        userRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('Source').isin(userSour)) & (col('Studios')==userStudio[0]) & (col('Genres').isin(userGen)) & (col('Rating').isin(userRating)) )\
            .orderBy(col('my_score').desc()).limit(5).rdd.map(lambda row:row[1]).collect()

    with open(r'UserRecomendation.txt', 'w') as fp:
        for item in userRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)
            
else:

    list_All=dfclean.select('username','anime_id','Genres').filter((col('Source').isin(userSour))  & (col('Genres').isin(userGen)) & (col('Rating').isin(userRating)))\
            .groupBy("username").count().rdd.map(lambda row:(row[0],row[1])).collect()

    with open(r'UserRecomendation.csv', 'w') as fp:
            fp.write('username,count')
            fp.write('\n')
            for item in list_All:
                # write each item on a new line
                for x in item:
                    fp.write(str(x)+',')
                fp.write('\n')
                
    local_path=os.path.abspath('UserRecomendation.csv')
    blob=bucket.blob("code/UserRecomendation.csv")
    blob.upload_from_filename(local_path)

    dfcount=spark.read.csv('UserRecomendation.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_All if t[1]==int(listMax[0])]
    if result:
        userRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('Source').isin(userSour))  & (col('Genres').isin(userGen)) & (col('Rating').isin(userRating)) )\
            .orderBy(col('my_score').desc()).limit(5).rdd.map(lambda row:row[1]).collect()

    with open(r'UserRecomendation.txt', 'w') as fp:
        for item in userRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)

print("Animes recomendados: \n")
print("Anime que coinciden con el genero introducido: \n")
print(genreRecomendation)
print('\n')
print("Anime que coinciden segun tu Source: \n")
print(sourceRecomendation)
print('\n')
print("Anime que coinciden segun tu Rating: \n")
print(ratingRecomendation)
print('\n')
if userStudio:
    print("Anime que coinciden con el estudio introducido: \n")
    print(studioRecomendation)
    print('\n')
if userRecomendation:
    print("Anime que coinciden con todos los datos introducidos: \n")
    print(userRecomendation)
    print('\n')
else:
    print("Disculpe pero no encontramos animes que cumplan esas caracteristicas")

# os.remove("genreUser.csv")
# os.remove("ratingUser.csv")
# os.remove("sourceUser.csv")
# os.remove("studioUser.csv")
# os.remove("UserRecomendation.csv")
# os.remove("userSource.txt")
# os.remove("userGeneros.txt")
# os.remove("userStudios.txt")
# os.remove("userRating.txt")

            
            
            