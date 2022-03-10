#!/usr/bin/env python
# coding: utf-8

# In[38]:


import pandas as pd
import json
import requests
API_key="051d30521795e4d2a16a29de43033af3"
get_ipython().system('ls data/zippedData | grep sv > file_list.txt')


# In[39]:


# get the list of tmdb movie id's

list_movie_ids=[]
list_errors=[]
with open('data/movie_ids_12_09_2021.json', 'r', encoding='utf-8') as f:

    for line in f:
        id_json=f.readline()#.strip()
        try:
#            list_movie_ids.append(json.loads(id_json))
            movie_id_json=json.loads(id_json)
        except:
            list_errors.append(line)
        else:
            list_movie_ids.append(movie_id_json)


# In[41]:


query = 'https://api.themoviedb.org/3/genre/movie/list?api_key='+API_key+'&language=en-US'
response =  requests.get(query)

pd.DataFrame(response.json()['genres']).to_csv("data/zippedData/tmdb_genres.csv.gz", encoding='utf-8', compression='gzip')


# In[42]:



list_movie_ids[1:3]


# In[43]:


# adding tmdb_movie_ids.csv.gz to zippedData folder

pd.DataFrame(list_movie_ids).to_csv("data/zippedData/tmdb_movie_ids.csv.gz", encoding='utf-8', compression='gzip')


# In[47]:


# get genre_info and save it in zippedData as gzip

query = 'https://api.themoviedb.org/3/genre/movie/list?api_key='+API_key+'&language=en-US'
response =  requests.get(query)

pd.DataFrame(response.json()['genres']).to_csv("data/zippedData/tmdb_genres.csv.gz", encoding='utf-8', compression='gzip')


# In[44]:


# loading all files into a dictionary

with open ('file_list.txt', 'r') as f:
    file_list= f.readlines()
    
csv_dict = {}

for csv in file_list:
    file='data/ZippedData/'+csv.strip()
    df_name=csv.strip()
    df_name=df_name.split('.csv.gz')[0]
    if 'csv' in file:
        df=pd.read_csv(file, compression='gzip')
    elif 'tsv' in file:
        df=pd.read_csv(file, compression='gzip', delimiter='\t', encoding='latin-1')
    else: print(file, ": unkown file!")
    csv_dict[df_name] = df


# In[45]:


# creating a list of in house tmdb id's:

id_list=list(csv_dict.get("tmdb.movies")["id"])
id_list=list(set(id_list))
len(id_list)


# In[46]:


# Define function to get tmdb details (budget,revenue, corresponding imdb id)


def get_data(API_key, Movie_ID):
    query = 'https://api.themoviedb.org/3/movie/'+Movie_ID+'?api_key='+API_key+'&language=en-US'
    response =  requests.get(query)
    if response.status_code==200: 

        array = response.json()
        return array
    else:
        return ("error")

Sample_MovieID="10138"

get_data(API_key, Sample_MovieID).keys()


# In[36]:


# get genre_info and save it in zippedData as gzip

query = 'https://api.themoviedb.org/3/genre/movie/list?api_key='+API_key+'&language=en-US'
response =  requests.get(query)

pd.DataFrame(response.json()['genres']).to_csv("data/zippedData/tmdb_genres.csv.gz", encoding='utf-8', compression='gzip')


# In[ ]:


## Uncomment this section to download imdb_id/tmdb_id information as well as revenue/budget information from API:
tmdb_list=[]
tmdb_dic={}
#for num,movie_id in enumerate(id_list):
#    movie_json=get_data(API_key, str(movie_id))
#    if movie_json != "error":
#        tmdb_dict = {"id":movie_json["id"], "tconst":movie_json["imdb_id"], "release_date":movie_json["release_date"], 
#                     "revenue": movie_json["revenue"], "budget": movie_json["budget"]}
#        tmdb_list.append(tmdb_dict)
####        if num == 200:
####            break


# pd.DataFrame(tmdb_list).to_csv("data/zippedData/tmdb_imdb_gross.csv.gz", encoding='utf-8', compression='gzip')

