#!/usr/bin/python3
import os
from pyspark import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *


conf=SparkConf().setAppName('ProjectAnimeFilter')
sc= SparkContext(conf=conf)
spark=SparkSession.builder.appName('PySpark Read CSV').getOrCreate()

df=spark.read.csv('clean.csv',sep=',',mode="DROPMALFORMED",header=True)
df1=spark.read.csv('Animes.csv',sep=',',mode="DROPMALFORMED",header=True)
df2=spark.read.csv('UserList.csv',sep=',',mode="DROPMALFORMED",header=True)

dfJoin=df.join(df1,df.anime_id==df1.MAL_ID,"inner")

dfclean=dfJoin.drop("MAL_ID")
df_names=df2.select("username")
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

#Recomendacion de Studio
########################################################################
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

    dfcount=spark.read.csv('studioUser.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_Studio if t[1]==int(listMax[0])]


    studioRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('Studios')==userStudio[0])).orderBy(col('my_score').desc())\
        .limit(5).rdd.map(lambda row:row[1]).collect()

    with open(r'StudioRecomendation.txt', 'w') as fp:
        for item in studioRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)

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

    dfcount=spark.read.csv('sourceUser.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_Source if t[1]==int(listMax[0])]


    sourceRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('Source').isin(userSour))).orderBy(col('my_score').desc())\
        .limit(5).rdd.map(lambda row:row[1]).collect()

    with open(r'SourceRecomendation.txt', 'w') as fp:
        for item in sourceRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)

# # #Recomendacion por Rating
# # #########################################################################
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

    dfcount=spark.read.csv('ratingUser.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_Rating if t[1]==int(listMax[0])]

    ratingRecomendation=dfclean.select("username",'Name').filter((col('username')==result[0]) & (col('rating').isin(userRating))).orderBy(col('my_score').desc())\
        .limit(5).rdd.map(lambda row:row[1]).collect()

    with open(r'RatingRecomendation.txt', 'w') as fp:
        for item in ratingRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)

# #Recomendacion por genre
# #########################################################################
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

    dfcount=spark.read.csv('genreUser.csv',sep=',',mode="DROPMALFORMED",header=True)
    listMax=dfcount.select(max("count")).rdd.map(lambda row:row[0]).collect()
    result=[t[0] for t in list_Gen if t[1]==int(listMax[0])]
    genreRecomendation=dfclean.select('Name','my_score').filter((col('username')==result[0]) & (col('Genres').isin(userGen))).orderBy(col('my_score').desc())\
        .limit(5).rdd.map(lambda row:(row[0])).collect()
        
    with open(r'GenreRecomendation.txt', 'w') as fp:
        for item in genreRecomendation:
            # write each item on a new line
            fp.write("%s\n" % item)

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

    dfcount=spark.read.csv('UserRecomendation.csv',sep=',',mode="DROPMALFORMED",header=True)
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
            
#Visualizacion de resultados
############################################################

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
    print("Disculpe pero no encontramos animes que cumplan esas caracteristicas\n")

os.remove("genreUser.csv")
os.remove("ratingUser.csv")
os.remove("sourceUser.csv")
os.remove("studioUser.csv")
os.remove("UserRecomendation.csv")
os.remove("userSource.txt")
os.remove("userGeneros.txt")
os.remove("userStudio.txt")
os.remove("userRating.txt")

