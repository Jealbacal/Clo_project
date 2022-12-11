# Recomendador de anime 

## Descripción
Nuestro proyecto trata sobre un recomendador de anime que proporciona al usuario el anime que más le puede interesar en función de varios criterios que el usuario introduce previamente. Los criterios para realizar la búsqueda son género, source(de donde vienen el anime, es decir de un libro, una novela, etc), rating (Pegi de edad) y el estudio que anima dicho anime.

Para recomendarlo utilizamos los .csv que aparecen en el repo, los cuales estan sacados de kaggle.

## Preparación del entorno

Para poder ejecutar los diversos scripts es necesario tener python instalado, por lo general suele venir pre-instalado en el sistema por lo que para ver la versión más actulizada ejecutamos el siguente comando : $ sudo apt update

##  Cómo ejecutar la aplicación

Lo primero que debemos hacer para ejecutar la aplicación es


##Módulos necesarios

Para los scripts que hemos utilizado se necesitán instalar los siguientes módulos.

* Módulo pyspark
* Módulo os
* Módulo pandas
* Módulo inquierer

Para poder instalar los siguientes módulos hay que seguir los siguientes pasos:

Primero abrir la terminal de python obteninedo $ python y después usar el comando $pip install <nombre_del_módulo> , una vez instalado siusamos
el comando import <nombreDelModulo> veremos que nos mostrará por terminal que ya lo tenemos.

##Instalación de Pyspark

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



# Clo_project
# Enlace a la dataset de los usuarios en kaggle, en especifico el UserAnimeList.csv
# https://www.kaggle.com/datasets/azathoth42/myanimelist
