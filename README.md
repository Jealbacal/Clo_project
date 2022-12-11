# Recomendador de anime 

## Descripción
Nuestro proyecto trata sobre un recomendador de anime que proporciona al usuario el anime que más le puede interesar en función de varios criterios que el usuario introduce previamente. Los criterios para realizar la búsqueda son género, source(de donde vienen el anime, es decir de un libro, una novela, etc), rating (Pegi de edad) y el estudio que anima dicho anime.
Para hacer la recomendación vamos a buscar entre los miles de usuarios que tenemos, aquellos que más animes hayan visto que coincidan con las caraterísitcas que pide el usuario. Esto es interesatne ya que, si le mostramos los 5 animes que más le han gustado al usuario que más animes de ese estilo ha visto , probablmente sean del agrado del usuario que esta utilizando nuestra aplicación(para ver cuales son los 5 que más le han gustado, cogemos los 5 que tienen un score más alto puesto por ese usuario).

Para recomendarlo utilizamos los .csv que aparecen en el repo, los cuales estan sacados de kaggle.

## Preparación del entorno

Para poder ejecutar los diversos scripts es necesario tener python instalado, por lo general suele venir pre-instalado en el sistema por lo que para ver la versión más actulizada ejecutamos el siguente comando : $ sudo apt update

##  Cómo ejecutar la aplicación

Lo primero que debemos hacer para ejecutar la aplicación es


## Módulos necesarios

Para los scripts que hemos utilizado se necesitán instalar los siguientes módulos.

* Módulo pyspark
* Módulo os
* Módulo pandas
* Módulo inquierer

Para poder instalar los siguientes módulos hay que seguir los siguientes pasos:

Primero abrir la terminal de python obteninedo $ python y después usar el comando $pip install <nombre_del_módulo> , una vez instalado siusamos
el comando import <nombreDelModulo> veremos que nos mostrará por terminal que ya lo tenemos.

## Instalación de Pyspark

Para usarlo debemos previamente instalarnos Java, para ello introducimos los siguientes comandos:

sudo apt install default-jre

Después para comprobar la isntalación usamos el siguiente comando:

java -version

Una vez hemos instalado y comprobado la versión de java, procedemos a intalar spark.
Primero descargamos y extraemos la distribución de Apache Spark a /usr/local/spark:

  curl -O https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
  tar xvf spark-3.3.1-bin-hadoop3.tgz
  sudo mv spark-3.3.1-bin-hadoop3 /usr/local/spark
 
Después actualizamos la variable PATH en el fichero ~/.profile y en nuestra sesión actual:

  echo 'PATH="$PATH:/usr/local/spark/bin"' >> ~/.profile
  source ~/.profile

Finalmente comprobamos que la instalación se ha realizado correctamente probando lo siguiente: 

  spark-submit /usr/local/spark/examples/src/main/python/pi.py 10

Si todo se ha instalado correctamente debería salirnos una línea diciendo : Pi is roughly 3.142480

Una vez sabemos que está bien instalado podemos porceder a ejecutar los scripts del proyecto con el siguiente comando (Hay que tener en cuenta que el fichero usuario.py es el único que no usa pyspark por lo que ese se ejecutario como un script de python normal):

spark-submit <nombre_fichero.py>

## Explicación de los scripts y de los csv

Primero vamos a hablar un poco de los archivos .csv que utilizamos:
* AnimeList.csv: contiene todos los animes con todos los atributos que tiene cada uno y a partir de los cuales vamos a realizar el filtrado.
* userList.csv: contiene una gran número de usaurios incluyendo los animes que se han visto y la nota que le han puesto.

A continuación vamos a comentar un poco de que se encarga cada script:

* pr.py : este es el script que ejecutaremos en primero lugar. Se encarga de leer el AnimeList.csv que contiene todos los datos de todos los animes para poder luego darle opciones de elección a los usuarios.
    - Para ver que es lo que puede elegir el usaurio creamos ficheros .txt para cada una de las opciones que le damos, es decir, creamos un    Studios.txt para los studios, un Source.txt para la source, un Genres.txt para los géneros y un Rating.txt para el rating. De esta forma en el usaurio.py podemos pedirle sus opciones.
    
* usuario.py : este script se encarga de pedir, mostrar y guardar las elecciones que ha decidido tomar el usuario a la hora de pedir que le recomendemos un anime. Puede decidir que género, source y rating quiere además de poder también elegir o no si quiere un studio en particular.
    - Para realizar las elecciones se usa flechita hacia abajo para moverse, hacia la derecha para seleccionar, hacia la izquierda para quitar la selección y al enter para guardar tu elección.
    - Los datos que ha seleccionado se guardan en ficheros .txt para que pueden ser leidos por el filter.py. Se crearan tantos ficheros como elecciones haya realizado el usaurio.

* filter.py: este script es el que se encarga de realizar el filtrado de los datos en función de las elecciones del usuario. Este script trabaja con el userList.csv, recorriendo todos los usuarios y contando la cantidad de animes que se han visto que coinciden con las elecciones del usaurio. Una vez tenemos ese conteo hecho, nos quedamos con el que más tiene y dentro de él escogemos los 5 que más nota tengan puesta por ese usuario para mostrarselo al usuario que pidió la recomendación (Para poder mostrar le anime y verlo hacemos un join con el otro csv que utilizamos).
  - Para mostrarle los resultados le mostramos los resultados de cada campo en su txt correspondiente llamado generRecomendation.txt para generos tal tal tal y finalmente un userRecomendation.txt con aquellos animes que incluyen todos los campos que ha introducido (puede ser que no haya ningún anime que contenga todos los campos introducidos por el usaurio, por eso también mostramos un .txt por cada eleccion hecha por el usaurio, es decir un txt de animes recomendados según el género, un txt según la source, etc.).
  
## Ejecutarlo en Google Cloud

    

    

# Clo_project
# Enlace a la dataset de los usuarios en kaggle, en especifico el UserAnimeList.csv
# https://www.kaggle.com/datasets/azathoth42/myanimelist
