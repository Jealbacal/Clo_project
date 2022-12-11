import inquirer
import pandas as pd


df = pd.read_csv('animeList.csv')
#Elimino columnas useless
df.drop(['Score','English name','Japanese name','Type','Episodes','Aired','Premiered','Producers','Licensors','Duration','Popularity','Members','Favorites',
         'Watching','Completed','On-Hold','Dropped','Plan to Watch','Score-10','Score-9','Score-8','Score-7','Score-6','Score-5','Score-4','Score-3','Score-2','Score-1'],
axis=1,inplace=True)
df['Studios'] = df['Studios'].str.lower()
df.to_csv('animes.csv',index=False)

df1 = pd.read_csv('UserAnimeList.csv')
#Elimino columnas useless
df1.drop(['my_watched_episodes','my_start_date','my_finish_date','my_rewatching','my_rewatching_ep','my_last_updated','my_tags'],axis=1,inplace=True)
df1.to_csv('clean.csv',index=False)

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