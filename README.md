# Recomendador de anime 

## Descripción
Nuestro proyecto trata sobre un recomendador de anime que proporciona al usuario animes que le puede interesar en función de unas elecciones realizadas sobre diversos atributos de los animes. Los criterios para realizar la búsqueda y elección son: género, source(de donde vienen el anime, es decir de un libro, una novela, etc), rating (Pegi de edad) y el estudio que anima dicho anime.

Para hacer la recomendación vamos a buscar entre los miles de usuarios que tenemos, aquellos que más animes hayan visto que coincidan con las caraterísitcas que pide el usuario. Esto es interesante ya que, si le mostramos los 5 animes que más le han gustado al usuario que más animes de ese estilo ha visto , probablemente estos le gusten más al usuario que esta utilizando nuestra aplicación que otros que le podamos recomendar nosotros de manera aleatoria en función de las elecciones realizadas (para ver cuales son los 5 que más le han gustado, cogemos los 5 que tienen un score más alto puesto por ese usuario).

Para recomendarlo utilizamos los .csv que aparecen en el repo, los cuales estan sacados de kaggle y de un repo ajeno de github.

Enlace a la página web:
# https://gavilaneees.github.io/

## Preparación del entorno

Para poder ejecutar los diversos scripts es necesario tener python instalado, por lo general suele venir pre-instalado en el sistema, por lo que para ver la versión más actulizada ejecutamos el siguente comando :  

```bash
sudo apt update
```
Si necesitáramos instalarlo, tendríamos que usar el comando : 

```bash
sudo apt-get install python3.6 
```
Finalmente para comprobar la versión que tenemos usamos el comando :

```bash
python3 –version 
```
Cómo para cargar los módulos vamos a necesitar pip, debemos instalarlo usando el siguiente comando :

```bash
sudo apt-get install python3-pip
```
A continuación solo nos quedaría instalar pyspark y los módulos necesarios.

## Instalación de Pyspark

Para usarlo debemos previamente instalarnos Java, para ello introducimos los siguientes comandos:

```bash
sudo apt install default-jre
```
Después para comprobar la isntalación usamos el siguiente comando:

```bash
java -version
```
Una vez hemos instalado y comprobado la versión de java, procedemos a intalar spark.
Primero descargamos y extraemos la distribución de Apache Spark a /usr/local/spark:

  ```bash 
  curl -O https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
  tar xvf spark-3.3.1-bin-hadoop3.tgz
  sudo mv spark-3.3.1-bin-hadoop3 /usr/local/spark
 ```
 
Después actualizamos la variable PATH en el fichero ~/.profile y en nuestra sesión actual:

  ```bash
  echo 'PATH="$PATH:/usr/local/spark/bin"' >> ~/.profile
  source ~/.profile
  ```
Finalmente comprobamos que la instalación se ha realizado correctamente probando lo siguiente: 

  ```bash
  spark-submit /usr/local/spark/examples/src/main/python/pi.py 10
  ```
Si todo se ha instalado correctamente debería salirnos una línea diciendo : Pi is roughly 3.142480

Una vez sabemos que está bien instalado podemos porceder a ejecutar los scripts del proyecto con el siguiente comando (Hay que tener en cuenta que el fichero usuario.py es el único que no usa pyspark por lo que ese se ejecutario como un script de python normal):

```bash
spark-submit <nombre_fichero.py>
```

## Módulos necesarios

Para los scripts que hemos utilizado se necesitán instalar los siguientes módulos.

* Módulo pyspark
* Módulo os
* Módulo pandas
* Módulo inquirer

Para poder instalar los siguientes módulos hay que seguir los siguientes pasos:

Primero abrir la terminal de python obteninedo $ python y después usar el comando
```bash 
pip install <nombre_del_módulo>
```

Una vez instalado si usamos este comando veremos que nos mostrará por terminal que ya lo tenemos.
```bash
import <nombreDelModulo> 
```

##  Cómo ejecutar la aplicación

Lo primero que debemos hacer para ejecutar la aplicación es ejecutar el script de pr.py para obtener los atributos que queremos que el usuario seleccione, para ello usamos este comando :
```bash
spark-submit pr.py
```
Una vez ejecutado ese script, pasamos a ejecutar el usuario.py que le pedirá al usuario que seleccione los atributos que quiere que tenga su anime. Finalmente le mostraremos y guardaremos en ficheros .txt sus elecciones. Es un simple fichero de python por lo que se ejecuta como tal.

Para finalizar ejecutamos el script filter.py que es el que se encarga de, en función de las elecciones del usuario, buscar entre los miles de usuarios a aquel que más animes que coincidan con dichas elecciones se haya visto, para así, finalmente mostrale por pantalla y por ficheros llamados XRecomendation.txt los animes que le recomendamos en función de cada parametro elegido (le recomendamos los 5 que más le hayan gustado al usuario del que los sacamos).
Para ejecutar el filter.py usamos :
```bash
spark-submit filter.py
```

## Explicación de los scripts y de los csv

Primero vamos a hablar un poco de los archivos .csv que utilizamos:
* AnimeList.csv: contiene todos los animes con todos los atributos que tiene cada uno y a partir de los cuales vamos a realizar el filtrado. De aquí sacamos el nombre.
* userAnimeList.csv: contiene una gran número de usaurios incluyendo los animes que se han visto y la nota que le han puesto.
* userList.csv: contiene una lista de todos los usuarios, incluyendo sus datos personales y un pequeño conteo de los animes que ha visto, que planea ver o que ha dejado de ver

A continuación vamos a comentar un poco de que se encarga cada script:

* pr.py : este es el script que ejecutaremos en primero lugar. Se encarga de leer el AnimeList.csv que contiene todos los datos de todos los animes para poder luego darle opciones de elección a los usuarios.
    - Para ver que es lo que puede elegir el usuario, creamos ficheros .txt para cada una de las opciones que le damos, es decir, creamos por ejemplo un Studios.txt para los estudios, un Source.txt para la source, un Genres.txt para los géneros y un Rating.txt para el rating. De esta forma en el usaurio.py podemos pedirle sus opciones.
    
* limpieza.py : este es el script que se encarga de limpiar los dataframes,en el usamos la libreria de pandas para eliminar las columnas que         consideramos inneserarias.

* usuario.py : este script se encarga de pedir, mostrar y guardar las elecciones que ha decidido tomar el usuario a la hora de pedir que le recomendemos un anime. Puede decidir que género, source y rating quiere, además de poder también elegir o no si quiere un estudio de animación en particular.
    - Para realizar las elecciones se usa flechita hacia abajo para moverse, hacia la derecha para seleccionar, hacia la izquierda para quitar la selección y al enter para guardar tu elección.
    - Los datos que ha seleccionado se guardan en ficheros .txt para que pueden ser leidos por el filter.py. Se crearan tantos ficheros como elecciones haya realizado el usuario.
    

* filter.py: este script es el que se encarga de realizar el filtrado de los datos en función de las elecciones del usuario. Este script trabaja con el userList.csv, recorriendo todos los usuarios y contando la cantidad de animes que se han visto que coinciden con las elecciones del usuario.
Una vez tenemos ese conteo hecho, nos quedamos con el que más tiene y dentro de su lista de animes escogemos los 5 que más nota tiene para mostrarselo al usuario que pidió la recomendación (Para poder mostrar el anime y verlo hacemos un join con animes.csv y clean.csv a través de la columna MAL_id).
    - Para mostrarle los resultados le mostramos los resultados de cada campo en su txt correspondiente llamado GenreRecomendation.txt para generos, SourceRecomendation para Source,StudioRecomendation  para los Estudiostal, RatingRecomendation para el Rating  y finalmente un UserRecomendation.txt con aquellos animes que incluyen todos los campos que ha introducido (puede ser que no haya ningún anime que contenga todos los campos introducidos por el usuario, por eso también mostramos un .txt por cada elección hecha por el usuario, es decir, un txt de animes recomendados según el género, un txt según la source, etc.).
  
## Ejecutarlo en Google Cloud
* Deberá descargar todos los archivos en local y ejecutar pr.py y user.py
* Una vez generado los archivos, deberá crear un BUCKET en cloud llamado "hdswa" y una carpeta llamada "code".
* Tras la creación del Bucket subirá todos los archivos allí.(Debería de tardar como 10 min)
* Para crar el cluster meterá el siguiente comando en la terminal de cloud:
 ```bash
 gcloud dataproc clusters create example-cluster --region europe-west6 --enable-component-gateway --master-boot-disk-size 50GB --worker-boot-disk-size 50GB
 ```
 * Ya creado el cluster, irá a Dataproc>Cluster>Cluster_info>Virtual Machines y se meterá en master
 * Subirá el archivo filter_cloud.py a la maquina virtual y podrá ejecutar el código con uno de los siguientes comandos:
 ```bash
 spark-submit --master local[4] filter_cloud.py
 spark-submit --num-executors 4 --executor-cores 4 filter_cloud.py
 ```
 Donde el numero 4 es modificable dependiendo de que rendimiento quiera.
    
# Clo_project
# Enlace a la dataset de los usuarios en kaggle, en especifico el UserAnimeList.csv y al repositorio de donde sacamos el AnimeList.csv de los animes, en especifico el animes.csv.
# https://www.kaggle.com/datasets/azathoth42/myanimelist
# https://github.com/Hernan4444/MyAnimeList-Database/blob/master/data/anime.csv
