Technical:

We did our research in several stages:

1. Stage 1 - figuring out and downloading all data
add_data2.py

- downloaded data from PMDB API containing 330,000 rows, with title, budget, revenue, popularity, genres, rating, number of votes, etc

-identified databases from imdb, rotten tomatos, and other movie databases 
(some of them provided by FlatIron School)



2. Stage 2 - Preparing data, cleaning and organizing it so it is ready for analysis
data_preparations.py

-merged data from different sources
-filled and/or removed NaN
-identified and dropped unusable data, etc
-created base dataframe full_profit_df



3. Stage 3 - Summarizing the data and visualizing it
Manipulate_Full_Data.ipynb

-created quesitons_specific dataframes based on full_profit_df

