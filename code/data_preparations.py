#!/usr/bin/env python
# coding: utf-8

# # Microsoft Recommendations

# In[780]:
# %run code/add_data.py
import numpy as np
import pandas as pd
pd.options.display.float_format = '{:,}'.format
from pandasql import sqldf
import json
import requests
import os
import seaborn as sns

os.system("ls data/zippedData | grep sv > file_list.txt")

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


# In[783]:

with open ('file_list.txt', 'r') as f:
    file_list= f.readlines()

csv_dict = {}

for csv in file_list:
    file='data/ZippedData/'+csv.strip()
    df_name=csv.strip()
    if 'csv' in file:
        df_name=df_name.split('.csv.gz')[0]
        df=pd.read_csv(file, compression='gzip')
    elif 'tsv' in file:
        df_name=df_name.split('.tsv.gz')[0]
        df=pd.read_csv(file, compression='gzip', delimiter='\t', encoding='latin-1')
    else: print(file, ": unkown file!")
    csv_dict[df_name] = df


# In[789]:
##########################################################################################
# Function to remove ',' and '$' from the value
##########################################################################################

def gross_to_num(gross_str):
    try:
        gross_num=float(gross_str.replace('$','').replace(',',''))

    except:
        return np.NaN
    else:
        return gross_num

##########################################################################################
# Function to extract year from forma like: "12 November, 2020"
##########################################################################################

def extract_year(year_string):
    if ',' in year_string:
        return year_string.split(',')[1].strip()
    else:
        return(year_string)

##########################################################################################
# Function to clean genre and remove "[" and "]"
##########################################################################################

def clean_genres(genre_str):
    filter_list=[]
    try:
        filter_str=genre_str.replace('''[''','').replace(''']''','')
    except:
        None
    else:
        if filter_str == '':
            filter_list=np.NaN
        else:
            filter_str=genre_str.replace('[','').replace(']','')
            filter_list=filter_str.split(',')
    return filter_list

##########################################################################################
#function to extract numbers in Runtime field:
##########################################################################################

def clean_runtime(runtime): 
    minutes = runtime.split()[0]
    return int(minutes)


##########################################################################################
# In[790]:

csv_dict.get('tmdb_imdb_gross_full').drop(columns=["Unnamed: 0"], inplace=True)

csv_dict["tmdb_imdb_gross_full"]["year"]=csv_dict["tmdb_imdb_gross_full"]["release_date"].map(lambda x: str(x)[0:4])
tmdb_imdb_gross_full=csv_dict["tmdb_imdb_gross_full"]
#/tmdb_imdb_gross=csv_dict["tmdb_imdb_gross"]

sql="""select cast(id as integer) id, tconst, title, release_date,
    cast(revenue as integer) revenue , cast(budget as integer) budget , 
    cast(vote_average as integer) vote_average, cast(popularity as integer) popularity, 
    cast(vote_count as integer) vote_count, cast(runtime as integer) runtime, 
    genres, year
    from tmdb_imdb_gross_full
    where  popularity!='popularity';"""

tmdb_budget=sqldf(sql,globals())

tmdb_budget.head()


# In[791]:

# prepairing gross columns in tn.movie_budgets, tmbdb_imdb_gross, and bom.movie_gross
csv_dict["tn.movie_budgets"]["tn_worldwide_gross"]=csv_dict["tn.movie_budgets"]["worldwide_gross"].apply(gross_to_num)
csv_dict["tn.movie_budgets"]["tn_production_budget"]=csv_dict["tn.movie_budgets"]["production_budget"].apply(gross_to_num)
csv_dict["tn.movie_budgets"]["tn_net_profit"]=csv_dict["tn.movie_budgets"]["tn_worldwide_gross"]-csv_dict["tn.movie_budgets"]["tn_production_budget"]

csv_dict["bom.movie_gross"]["foreign_gross_fixed"]=csv_dict["bom.movie_gross"].loc[:,"foreign_gross"].apply(gross_to_num)
csv_dict["bom.movie_gross"]["foreign_gross_fixed"].replace(np.NaN,0, inplace=True)
csv_dict["bom.movie_gross"]["domestic_gross"].replace(np.NaN,0, inplace=True)
csv_dict["bom.movie_gross"]["bom_total_gross"]=csv_dict["bom.movie_gross"]["domestic_gross"] + csv_dict["bom.movie_gross"]["foreign_gross_fixed"]


# In[562]:

csv_dict.get("tn.movie_budgets")["release_date"]=csv_dict.get("tn.movie_budgets")["release_date"].apply(extract_year)
csv_dict["bom.movie_gross"]["year"]=csv_dict["bom.movie_gross"]["year"].map(lambda x: str(x))  

# In[667]:

temp1_df=tmdb_budget.merge(csv_dict.get("tn.movie_budgets")[["movie", "tn_production_budget", "tn_net_profit", "tn_worldwide_gross", "release_date" ]],                            left_on=['title','year'], right_on=["movie","release_date"], how="left")

full_merge_df=temp1_df.merge(csv_dict.get("bom.movie_gross")[["title","bom_total_gross","year"]],                             left_on=['title','year'], right_on=["title","year"], how="left")

# In[668]:

full_merge_df["tn_worldwide_gross"].replace(0,np.NaN, inplace=True)
full_merge_df["bom_total_gross"].replace(0,np.NaN, inplace=True)
full_merge_df["tn_production_budget"].replace(0,np.NaN, inplace=True)
full_merge_df["tn_net_profit"].replace(0,np.NaN, inplace=True)
#full_merge_df["tmdb_net_profit"].replace(0,np.NaN, inplace=True)
full_merge_df["budget"].replace(0,np.NaN, inplace=True)
full_merge_df["revenue"].replace(0,np.NaN, inplace=True)

# In[671]:

full_merge_df["budget"].fillna(full_merge_df["tn_production_budget"], inplace=True)
full_merge_df["revenue"].fillna(full_merge_df["tn_worldwide_gross"], inplace=True)
full_merge_df["revenue"].fillna(full_merge_df["bom_total_gross"], inplace=True)
full_merge_df["year"].fillna(full_merge_df["release_date_y"], inplace=True)


full_merge_df.dropna(subset=["revenue"], inplace=True)
full_merge_df["tmdb_net_profit"]  = full_merge_df["revenue"]-full_merge_df["budget"]

# In[672]:

sql="""
select id, tconst, title,  release_date_x release_date, revenue, budget,  tmdb_net_profit net_profit, cast(year as integer) year, 
vote_average, popularity, vote_count, runtime, genres
from full_merge_df
"""

full_profit_df=sqldf(sql, globals())

######################################################################################################################

# In[720]:

full_profit_df["genre_id"]=full_profit_df['genres'].apply(clean_genres)
full_profit_df.drop(columns='genres', inplace=True)
full_profit_by_genre_id=full_profit_df.explode('genre_id')

csv_dict["tmdb_genres"]["id"]=csv_dict["tmdb_genres"]["id"].astype(str)

full_profit_by_genre_id.dropna(subset=['genre_id'], inplace=True)

full_profit_by_genre=full_profit_by_genre_id.merge(csv_dict["tmdb_genres"], left_on=['genre_id'], right_on='id')
full_profit_by_genre.drop(columns=["Unnamed: 0","id_y"], inplace=True)
full_profit_by_genre.rename(columns={"name": "genre", "id_x":"id"}, inplace=True)

# In[721]:

director_nconst=csv_dict.get("imdb.title.principals").query("category == 'director'")
full_profit_nconst_df=full_profit_df.merge(director_nconst[["tconst","nconst"]], on=["tconst"])
full_profit_dir_df=full_profit_nconst_df.merge(csv_dict.get("imdb.name.basics")[["nconst","primary_name"]], on=["nconst"])


# In[746]:
# Joining by Director and minutes/year
######################################################################################################################

# type_info = csv_dict['rt.movie_info'][['id', 'director', 'runtime', 'rating']]

# name_key = csv_dict['imdb.name.basics'][['nconst', 'primary_name']]
# dir_time = type_info.merge(name_key, left_on = 'director', right_on = 'primary_name')
# dir_time.dropna(subset=["runtime"], inplace=True)
# dir_time['runtime'] = dir_time['runtime'].apply(clean_runtime)
# dir_time.rating.notna().sum()
# 
# tconst_dir1=csv_dict['imdb.title.crew'][['tconst', 'directors']].copy()
# 
# movie_id_names_time = csv_dict['imdb.title.basics'][['tconst', 'primary_title', 'runtime_minutes']].copy()
# movie_id_names_time.dropna(subset=["runtime_minutes"], inplace=True)
# 
# tconst_dir = movie_id_names_time.merge(tconst_dir1, how = 'inner', on = 'tconst')
# tconst_dir.dropna(subset=["directors"], inplace=True)
# 
# tconst_dir['director']= tconst_dir['directors'].str.split(',')
# tconst_dir.drop(columns = 'directors', inplace = True)
# tconst_dir_each=tconst_dir.explode("director")

# In[749]:

# dir_time.dropna(subset=["runtime"], inplace=True)
# tconst_dir_time=dir_time.merge(tconst_dir_each, left_on=["nconst","runtime"], right_on=["director", "runtime_minutes"])
# tconst_dir_time.drop(columns = ['director_x','director_y'], inplace=True)

# full_profit_merge=full_profit_df.merge(tconst_dir_time[['runtime', 'rating', 'primary_name','tconst']], on=['tconst'], how="left")

