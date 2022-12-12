import inquirer
import pandas as pd

#lista generos para mostrar al usuario y que tenga opción multiple
listaGeneros=[]
questions = [
  inquirer.Checkbox(name= 'genres',
                message="¿Qué generos te gustan?",
                choices=['Action','Adventure','Avant Garde','Award Winning','Boys Love', 'Comedy','Drama', 
    'Fantasy','Girls Love','Gourmet','Horror', 'Mistery', 'Romance', 'Sci-Fi', 'Slice of Life', 'Sports', 'Supernatural','Suspense','Shounen','Seinen','Josei','Shoujo'],
            ),
]
answers = inquirer.prompt(questions)

for i in answers["genres"]:
    listaGeneros.append(i)



#lista sources para opcion multiple del usuario
listaSource=[]
questions2 = [
  inquirer.Checkbox(name= 'sources',
                message="¿De donde prefieres que venga el anime?",
                choices=['Web manga','Novel','Light novel','Book','Music','Card game','Original','Game','4-koma manga','Digital manga','Picture book','Manga','Radio','Other','Visual novel'],
            ),
]
answers2 = inquirer.prompt(questions2)
for i in answers2["sources"]:
    listaSource.append(i)




#lista rating para opcion multiple del usuario
listaRating=[]
questions3 = [
  inquirer.Checkbox(name= 'rating',
                message="¿Alguna preferencia de edad?",
                choices=['G - All Ages','PG-13 - Teens 13 or older','PG - Children','R - 17+ (violence & profanity)','R+ - Mild Nudity'],
            ),
]
answers3 = inquirer.prompt(questions3)
for i in answers3["rating"]:
    listaRating.append(i)

#abro el fichero de estudios para ver si ese estudio esta en nuestra base de datos y los meto en una lista en minusculas todo y le quito el salto de linea con el strip
with open("Studios.txt") as file_in:
    studios = []
    for line in file_in:
        studios.append(line.lower().strip())

#le pido unout hasta que me diga algo que esta en el fichero de studios.txt
while True:
    ans = input("¿Hay algun estudio que te guste mucho? (si no hay ninguno escriba no): ")
    if ans.lower() in studios:
        name = True
        print("Entendido\n")
        break
    elif ans.lower() == 'no':
        name=True
        print("Entendido\n")
        break
    else:
        print("\n No encontramos ninguna serie que tenga ese estudio, pruebe a poner espacios entre las palabras o mirar bien como se llama el estudio\n") 



listaStudio=[]
ans = ans.lower()
listaStudio.append(ans)


if 'no' in listaStudio:
    listaStudio=[]


print("Estas son tus elecciones:\n")


print("Generos: ",listaGeneros)
with open(r'userGeneros.txt', 'w') as fp:
    for item in listaGeneros:
        fp.write("%s\n" % item)

print("Sources: ",listaSource)
with open(r'userSource.txt', 'w') as fp:
    for item in listaSource:
        fp.write("%s\n" % item)

print("Rating: ",listaRating)
with open(r'userRating.txt', 'w') as fp:
    for item in listaRating:
        fp.write("%s\n" % item)

if len(listaStudio)==0:
    print("No importa el estudio")
else:
    print("Estudio: ",listaStudio[0])
    with open(r'userStudio.txt', 'w') as fp:
        for item in listaStudio:
            fp.write("%s\n" % item)