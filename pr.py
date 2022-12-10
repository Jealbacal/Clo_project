#!/usr/bin/python3
import os
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructField, StructType, IntegerType


conf=SparkConf().setAppName('ProjectAnimeFilter')
sc= SparkContext(conf=conf)
spark=SparkSession.builder.appName('PySpark Read CSV').getOrCreate()
schema = StructType(
    [
        StructField('MAL_ID', IntegerType()),
        StructField('Name', StringType()),
        StructField('Score', StringType()),
        StructField('Genres', StringType()),
        StructField('English name', StringType()),
        StructField('Japanese name', StringType()),
        StructField('Type', StringType()),
        StructField('Episodes', StringType()),
        StructField('Aired', StringType()),
        StructField('Premiered', StringType()),
        StructField('Producers', StringType()),
        StructField('Licensors', StringType()),
        StructField('Studios', StringType()),
        StructField('Source', StringType()),
        StructField('Duration', StringType()),
        StructField('Rating', StringType()),
        StructField('Ranked', FloatType()),
        StructField('Popularity', IntegerType()),
        StructField('Members', IntegerType()),
        StructField('Favorites', IntegerType()),
        StructField('Watching', IntegerType()),
        StructField('Completed', IntegerType()),
        StructField('On-Hold', IntegerType()),
        StructField('Dropped', IntegerType()),
        StructField('Plan to watch', IntegerType()),
        StructField('Score-10', FloatType()),
        StructField('Score-9', FloatType()),
        StructField('Score-8', FloatType()),
        StructField('Score-7', FloatType()),
        StructField('Score-6', FloatType()),
        StructField('Score-5', FloatType()),
        StructField('Score-4', FloatType()),
        StructField('Score-3', FloatType()),
        StructField('Score-2', FloatType()),
        StructField('Score-1', FloatType()),
        
        



    ]
)
# df=spark.read.csv('AnimeList.csv',sep=',',mode="DROPMALFORMED",schema=schema)
# df1=df.groupBy("recclass").avg("mass (g)")
# df1.write.csv("output5.csv")
# lista=df1.rdd.map(lambda x:(x[0],x[1])).collect()
# print(lista)
df=spark.read.csv('animeList.csv',sep=',',mode="DROPMALFORMED",schema=schema)
lista=df.select('Genres').rdd.flatMap(lambda x:x).collect()
lista2=df.select('Studios').rdd.flatMap(lambda x:x).collect()
lista3=df.select('Source').rdd.flatMap(lambda x:x).collect()
lista4=df.select('Rating').rdd.flatMap(lambda x:x).collect()


# df3=df.select('Studios')
# df3.distinct().coalesce(1).write.format("text").option("header", "false").mode("append").save("Studios.txt")

# df4=df.select('Source')
# df4.distinct().coalesce(1).write.format("text").option("header", "false").mode("append").save("Source.txt")

# df5=df.select('Rating')
# df5.distinct().coalesce(1).write.format("text").option("header", "false").mode("append").save("Rating.txt")

# df6=df.select('Genres')
# df6.distinct().coalesce(1).write.format("text").option("header", "false").mode("append").save("Genres.txt")


#creo nuevo fichero y le meto cada elemtno de la lista en cada fila 

with open(r'Genres0.txt', 'w') as fp:
    for item in lista:
        # write each item on a new line
        fp.write("%s\n" % item)
    print('Done')

with open(r'Studios0.txt', 'w') as fp:
    for item in lista2:
        # write each item on a new line
        fp.write("%s\n" % item)
    print('Done')

with open(r'Source0.txt', 'w') as fp:
    for item in lista3:
        # write each item on a new line
        fp.write("%s\n" % item)
    print('Done')

with open(r'Rating0.txt', 'w') as fp:
    for item in lista4:
        # write each item on a new line
        fp.write("%s\n" % item)
    print('Done')

#Abro los ficheros y elimino los atributos unknown

# Read file.txt
with open('Genres0.txt', 'r') as file:
    text = file.read()


# Delete text and Write
with open('Genres0.txt', 'w') as file:
    # Delete
    new_text = text.replace('Unknown', '')
    # Write
    file.write(new_text)


# Read file.txt
with open('Studios0.txt', 'r') as file:
    text = file.read()


# Delete text and Write
with open('Studios0.txt', 'w') as file:
    # Delete
    new_text = text.replace('Unknown', '')
    # Write
    file.write(new_text)

with open('Source0.txt', 'r') as file:
    text = file.read()


# Delete text and Write
with open('Source0.txt', 'w') as file:
    # Delete
    new_text = text.replace('Unknown', '')
    # Write
    file.write(new_text)

# Read file.txt
with open('Rating0.txt', 'r') as file:
    text = file.read()


# Delete text and Write
with open('Rating0.txt', 'w') as file:
    # Delete
    new_text = text.replace('Unknown', '')
    # Write
    file.write(new_text)

#Elimino los espacios en blanco que han quedado de quitar aquellos que tenian como valor unknown

with open("Genres0.txt", 'r') as r, open('Genres1.txt', 'w') as o:
    for line in r:
        #strip() function
        if line.strip():
            o.write(line)

with open("Studios0.txt", 'r') as r, open('Studios1.txt', 'w') as o:
    for line in r:
        #strip() function
        if line.strip():
            o.write(line)

with open("Source0.txt", 'r') as r, open('Source1.txt', 'w') as o:
    for line in r:
        #strip() function
        if line.strip():
            o.write(line)

with open("Rating0.txt", 'r') as r, open('Rating1.txt', 'w') as o:
    for line in r:
        #strip() function
        if line.strip():
            o.write(line)
            
#Elimino los duplicados para poder asi pasarle una lista al usuario y que elija

with open('Genres1.txt') as result:
        uniqlines = set(result.readlines())
        with open('Genres.txt', 'w') as rmdup:
            rmdup.writelines(set(uniqlines)) 

with open('Studios1.txt') as result:
        uniqlines = set(result.readlines())
        with open('Studios.txt', 'w') as rmdup:
            rmdup.writelines(set(uniqlines)) 

with open('Source1.txt') as result:
        uniqlines = set(result.readlines())
        with open('Source.txt', 'w') as rmdup:
            rmdup.writelines(set(uniqlines))

with open('Rating1.txt') as result:
        uniqlines = set(result.readlines())
        with open('Rating.txt', 'w') as rmdup:
            rmdup.writelines(set(uniqlines))  

os.remove("Genres0.txt")
os.remove("Studios0.txt")
os.remove("Source0.txt")
os.remove("Rating0.txt")
os.remove("Genres1.txt")
os.remove("Studios1.txt")
os.remove("Source1.txt")
os.remove("Rating1.txt")

