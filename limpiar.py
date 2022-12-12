
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