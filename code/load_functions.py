
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
