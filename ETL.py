# extraction, transformation and loading of all csv files for a given date range
# the main date range is divided into batches of selected size
# Arguments :
#  1) start of the date range
#  2) end of the date range (excluded)
#  3) size of batches
# ATTENTION : Le choix de la connection à traiter se fait dans la fonction "connect_to_coll"


import pandas as pd
import numpy as np
import re
import pymongo
import pprint
import os
import pathlib
import sys
import datetime
import subprocess
import glob
import urllib.error


def connect_to_coll():
    # dans le string on peut passer les contraintes sur R et W
    conn_str = "mongodb://localhost:27017/"
    try:
        mongo_client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    except Exception:
        print("pymongo : unable to connect to the server.")
    db = mongo_client.gdelt
    coll = db.evt
    return coll


# -------------------FUNCTIONS FOR CLEANING DATA OF A SINGLE CSV FILE---------------------------- #

def clean_events(events_url):
    # cleaning data from an events_url provided as input
    
    # loading events from csv into pandas
    df_events=pd.read_csv(events_url, compression="zip", dtype=object, delimiter='\t',header=None,usecols=[0,1,53,34,26,27,28,31,32,37,45],encoding="ISO-8859-1")
    
    # renaming columns
    df_events=df_events[[0,1,53,34,26,27,28,31,32,37,45]]
    df_events=df_events.rename(columns=dict(zip([0,1,53,34,26,27,28,31,32,37,45],["ID","date","country","tone","theme","theme_base","theme_root","num_mentions","num_sources","act1_country","act2_country"])))
    
    # removing rows that have no ID
    df_events=df_events[df_events["ID"].notna()]

    # converting date to datetime and keep only date in 2021
    df_events.loc[:, 'date'] = pd.to_datetime(df_events.loc[:, 'date'], format='%Y%m%d')
    start = pd.to_datetime('2021-01-01')
    end = pd.to_datetime('2021-12-31')
    mask = (df_events['date'] >= start) & (df_events['date'] <= end)
    df_events = df_events.loc[mask]
    
    # converting numerical fields to numerical type (normaly performed automaticaly by pandas but here dtype was set to object in order to keep zeros in event codes
    df_events.loc[:, 'tone'] = df_events.loc[:, 'tone'].astype(float)
    df_events.loc[:, 'num_mentions'] = df_events.loc[:, 'num_mentions'].astype(int)
    df_events.loc[:, 'num_sources'] = df_events.loc[:, 'num_sources'].astype(int)
    df_events.loc[:, 'ID'] = df_events.loc[:, 'ID'].astype(int)
    
    # mapping of event codes with local mapping file
    data_folder = pathlib.Path(__file__).parent / 'data'
    s_mapping_code = pd.read_csv(data_folder / "CAMEO.eventcodes.new.txt",dtype='str').set_index('CAMEOEVENTCODE')["EVENTDESCRIPTION"]
#     df_events.loc[:, 'theme'] = df_events.loc[:, 'theme'].map(s_mapping_code)
    df_events = df_events.drop("theme",axis=1) # delete themes are 90% of them are identifcal to theme_base
    df_events.loc[:, 'theme_base'] = df_events.loc[:, 'theme_base'].map(s_mapping_code)
    df_events.loc[:, 'theme_root'] = df_events.loc[:, 'theme_root'].map(s_mapping_code)
    
    return df_events



def clean_gkg(gkg_url):
    # cleaning data from a gkg_url (path) provided as input
    
    # loading gkg from csv to pandas
    df_gkg = pd.read_csv(gkg_url, compression="zip", delimiter='\t', header=None, usecols=[4, 1, 3, 25, 9, 7, 15, 11, 13],encoding="ISO-8859-1")
    
    # renaming columns
    df_gkg=df_gkg[[4, 1, 3, 25, 9, 7, 15, 11, 13]]
    df_gkg=df_gkg.rename(columns=dict(zip([4, 1, 3, 25, 9, 7, 15, 11, 13],["ID","date","source","lang","locs","themes","tone","persons","org"])))
    
    # removing rows that have no ID
    df_gkg=df_gkg[df_gkg["ID"].notna()]

    #converting date to datetime and keep only date in 2021
    df_gkg.loc[:, 'date'] = pd.to_datetime(df_gkg.loc[:, 'date'], format="%Y%m%d%H%M%S")
    df_gkg.loc[:, 'date'] = pd.to_datetime(df_gkg.loc[:, 'date'].dt.strftime('%Y-%m-%d'))
    start = '2021-01-01'
    end = '2021-12-31'
    mask = (df_gkg['date'] >= start) & (df_gkg['date'] <= end)
    df_gkg = df_gkg.loc[mask]
    
    # parsing themes, persons and org to transform into array
    df_gkg["persons"]=df_gkg["persons"].str.split(';')
#     df_gkg["themes"]=df_gkg["themes"].str.split(';')
    df_gkg=df_gkg.drop("themes",axis=1)# themes are excluded - too mesy
    df_gkg["org"]=df_gkg["org"].str.split(';')
    
    # parsing locations, keeping only array of unique countries mentionned per article
    df_gkg["locs"]=df_gkg["locs"].apply(lambda x : list(set([elt.split('#')[2] for elt in x.split(';')])) if not pd.isna(x) else x)
    
    # parsing tone and keeping only first value
    df_gkg["tone"] = df_gkg.loc[:, "tone"].str.split(',')
    df_gkg["tone"] = df_gkg.loc[:, "tone"].apply(lambda x : x[0])
    
    # parsing language - null means no translation --> english
    df_gkg["lang"]=df_gkg["lang"].apply(lambda x : re.split(';|:',x)[1] if not pd.isna(x) else "eng")

    return df_gkg


def clean_mentions(mentions_url):
    # cleaning data from an mentions_url (path) provided as input
    
    # loading mentions database
    df_mentions=pd.read_csv(mentions_url, compression="zip", delimiter='\t',header=None,usecols=[0,5],encoding="ISO-8859-1")
    # renaming columns
    df_mentions=df_mentions.rename(columns=dict(zip([0,5],["event_ID","article_ID"])))
    # removing rows with either event_ID or article_ID missing
    df_mentions=df_mentions[df_mentions["event_ID"].notna() & df_mentions["article_ID"].notna()]
    
    return df_mentions




# ---------------------FUNCTIONS FOR EMBEDDING ARTICLES IN EVENT------------------------------- #


def gather_events(start, end):
    # cleaning and gathering all events for a given day and for both eng and translat files in one single dataframe
    #start/end format : "YYYY-MM-DD hh:mm:ss"
    
    tic_start = datetime.datetime.now()
    
    freq = "15min"
    dates = pd.date_range(start=start, end=end, freq=freq)[:-1]
    dates_url = dates.strftime('%Y%m%d%H%M%S')
    
    df_events = pd.DataFrame()
    # treating both english and translated data for the day
    for lang in ["eng","translat"]:
        for date in dates_url:
            
            if lang == "eng":
                url = f"http://data.gdeltproject.org/gdeltv2/{date}.export.CSV.zip"
            else: 
                url = f"http://data.gdeltproject.org/gdeltv2/{date}.translation.export.CSV.zip"
            # downloading dataframes from CSV urls and handling broken urls
            try:
                df_read = clean_events(url)
                df_events = pd.concat([df_events,df_read],axis=0)
#                 print(f"{url=} - df_events size : {len(df_events)}",end='\r')
            except urllib.error.HTTPError:
                url_id = url[37:]
                broken_urls.append(url_id)
                print(f"INFO : {url_id} could not be downloaded - batch continues")
    
    duration = datetime.datetime.now()-tic_start
    print(f"{len(df_events)} events cleaned and gathered in {str(duration)[:9]}")
    return df_events


def gather_events_articles_associations(start, end):
    # cleaning and gathering all events_articles association for a given day and for both eng and translat files in one single dataframe
    #start/end format : "YYYY-MM-DD hh:mm:ss"
    
    tic_start = datetime.datetime.now()
    
    freq = "15min"
    dates = pd.date_range(start=start, end=end, freq=freq)[:-1]
    dates_url = dates.strftime('%Y%m%d%H%M%S')
    
    df_articles = pd.DataFrame()
    for lang in ["eng","translat"]:
        # appending each files data into main dataframe
        for date in dates_url:
            if lang == "eng":
                url_mentions = f"http://data.gdeltproject.org/gdeltv2/{date}.mentions.CSV.zip"
                url_gkg = f"http://data.gdeltproject.org/gdeltv2/{date}.gkg.csv.zip"
            else: 
                url_mentions = f"http://data.gdeltproject.org/gdeltv2/{date}.translation.mentions.CSV.zip"
                url_gkg = f"http://data.gdeltproject.org/gdeltv2/{date}.translation.gkg.csv.zip"
            # downloading dataframes from CSV urls and handling broken urls
            gkg_downloaded = False
            mentions_downloaded = False
            try:
                df_gkg = clean_gkg(url_gkg)
                gkg_downloaded = True
            except urllib.error.HTTPError:
                url_id = url_gkg[37:]
                broken_urls.append(url_id)
                print(f"INFO : {url_id} could not be downloaded - batch continues")
            # downloading dataframes from CSV urls and handling broken urls
            try:
                df_mentions = clean_mentions(url_mentions)
                mentions_downloaded = True
            except urllib.error.HTTPError:
                url_id = url_gkg[37:]
                broken_urls.append(url_id)
                print(f"INFO : {url_id} could not be downloaded - batch continues")
            # if both files could be downloaded, they are integrated in the global batch db
            if gkg_downloaded & mentions_downloaded:
                df_merged = pd.merge(df_mentions,df_gkg,left_on='article_ID',right_on='ID')
                df_merged = df_merged.drop("article_ID",axis=1)
                df_articles = pd.concat([df_articles,df_merged],axis=0)
    
    # remove all duplicated event_ID - event_code pairs (these dupplicates come from mentions csv)
    df_articles = df_articles.loc[~df_articles.duplicated(subset=["event_ID","ID"])]
    
    duration = datetime.datetime.now()-tic_start
    print(f"{len(df_articles)} events-articles pairs cleaned and gathered in {str(duration)[:9]}")
    return df_articles



def articles_embedding(df_events,df_articles):
    
    start = datetime.datetime.now()
    
    # Events related objects preprocessing
    list_events = df_events.to_dict('records')
    dict_events={}
    for i in range(len(list_events)):
        list_events[i]["list_articles"]=[] # instanciation du champ list_articles avec liste vide
        dict_events[list_events[i]["ID"]] = list_events[i] # créer un dictionnaire contenant comme clé l'ID et comme value le dict de l'event
    
    # Articles related objects preprocessing
    list_articles = df_articles.iloc[:,1:].to_dict('records')
    list_evt_ID_articles = df_articles["event_ID"]
    
    # Delete nan fields as they are not useful in database and can cause issues in queries / pollute results
    for dict_article in list_articles:
        for key in ['locs','tone','persons','org']: # add themes to the list if article themes are included
            try :
                if pd.isna(dict_article[key]):
                    del dict_article[key]
            except:
                pass
    
    # embed articles when possible, add_to_update list if not
    list_evt_ID_articles_update = []
    list_articles_update = []
    nb_articles_embedded_in_pd = 0
    for i,id_evt in enumerate(list_evt_ID_articles):
        if id_evt in df_events["ID"].values:
            dict_events[id_evt]["list_articles"].append(list_articles[i])
            nb_articles_embedded_in_pd += 1
        else:
            list_evt_ID_articles_update.append(id_evt)
            list_articles_update.append(list_articles[i])
    
    duration = datetime.datetime.now()-start
    print(f"articles embedded in pandas : {nb_articles_embedded_in_pd} out of {len(list_evt_ID_articles)} events-articles associations in {str(duration)[:9]}")
    print(f"pandas embedding rate : {round(nb_articles_embedded_in_pd/len(list_evt_ID_articles)*100,1)} %")
            
    return list(dict_events.values()), list_evt_ID_articles_update, list_articles_update





# ---------------------FUNCTIONS FOR LOADING DATA IN MONGODB------------------------------- #

def load_docs_in_coll(coll,list_documents):  
    
    start = datetime.datetime.now()
    
    # load documents from pandas to mongodb
    result = coll.insert_many(list_documents)
    
    duration = datetime.datetime.now()-start
    nb_insertions = len(result.inserted_ids)
    print(f"* {nb_insertions} documents inserted in coll - completed in {str(duration)[:9]}")


def update_coll_docs_with_subdocs(coll,list_doc_ID_subdocs_update, list_subdocs_update):
    
    start = datetime.datetime.now()

    # list that contains the doc_ID associated to each of the subdocs
    list_doc_ID_subdocs_update_compressed = np.unique(list_doc_ID_subdocs_update)
    list_subdocs_update_compressed = dict(zip(list_doc_ID_subdocs_update_compressed,[[] for elt in list_doc_ID_subdocs_update_compressed]))
    for i,id_evt in enumerate(list_doc_ID_subdocs_update):
        list_subdocs_update_compressed[id_evt].append(list_subdocs_update[i])
    print(f"{len(list_doc_ID_subdocs_update)} document_subdocument associations concerning {len(list_doc_ID_subdocs_update_compressed)} distinct documents")
    # updating the database
    nb_updates = 0
    for i,elt in enumerate(list_doc_ID_subdocs_update_compressed):
        update = coll.update_many({"ID":int(elt)},{"$push":{"list_articles": {"$each" : list_subdocs_update_compressed[int(elt)]}}})
        nb_updates += update.modified_count
#         if i%50 == 0: # display information about progression (not usefull anymore as this step is now quick enough)
#             print(f"processing item {i} over {len(list_doc_ID_subdocs_update_compressed)} items in total - {round(i/len(list_doc_ID_subdocs_update_compressed)*100,1)} %",end='\r')
            
    duration = datetime.datetime.now()-start
    print(f"* {nb_updates} events updated out of {len(list_doc_ID_subdocs_update_compressed)} - completed in {str(duration)[:9]}  ")
    print(f"updates rate is {round(nb_updates/len(list_doc_ID_subdocs_update_compressed)*100,1)} %")

    
    
# ----------------------MAIN FUNCTION ORCHESTRATING OPERATIONS -------------------------------#
        
if __name__ == "__main__" :
    
    global_start = datetime.datetime.now()
    
    print("\nPLEASE ENSURE TO HAVE FORWORDED LOGS TO DEDICATED LOGS FILE !!!!!\n")
    
    # connect to database
    try:
        coll
    except:
        coll = connect_to_coll()
    
    date_min = str(sys.argv[1])
    date_max = str(sys.argv[2])
    
    # the global date_range is divided into sub ranges (due to monitoring and RAM capacity reasons)
    batch_size = int(sys.argv[3])
    
    print("\n####################################################################################")
    print(f"Rename current logs file with following name :\n{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')}_batch_{date_min}_{date_max}_coll_{str(coll.full_name).replace('.','-')}.logs")
    print("####################################################################################")
    
    print(f"\nPROCESS STARTED : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"DATE RANGE : {date_min} --> {date_max}")
    print(f"TARGET COLLECTION : {coll.full_name}")
    print(f"COLLECTION INDEXES : {coll.index_information()}\n")
    
    date_range = pd.date_range(date_min,date_max)
    
    pos=0
    broken_urls=[]
    
    while pos < (len(date_range)-1):
        
        batch_start = date_range[pos]
        try:
            batch_stop = date_range[pos+batch_size]
        except:
            batch_stop = date_range[-1] # if index out of range, sub_range finishes at the last element of the global range
            
            
        print(f"|||{datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')}--------- PROCESSING BATCH : {batch_start.strftime('%Y/%m/%d')}-{batch_stop.strftime('%Y/%m/%d')} - global range : {date_min}-{date_max} -----------|||")
        
        
        print("preprocessing events and articles")
        df_events = gather_events(batch_start,batch_stop)
        df_articles = gather_events_articles_associations(batch_start,batch_stop)

        print("pandas embedding")
        list_events, list_evt_ID_articles_update, list_articles_update = articles_embedding(df_events,df_articles)

        print(f"loading {len(list_events)} events in MongoDB collection")
        load_docs_in_coll(coll,list_events)

        print(f"loading {len(list_evt_ID_articles_update)} embedded articles in MongoDB collection")
        update_coll_docs_with_subdocs(coll,list_evt_ID_articles_update, list_articles_update)
        
        pos+=batch_size
    
    global_duration = datetime.datetime.now()-global_start
    print(f"\nETL EXECUTED SUCCESFULLY FROM {date_min} TO {date_max} IN {global_duration}")
    if len(broken_urls)>0:
        print("Following files could no be downloaded due to broken url :")
        for broken_url in broken_urls:
            print(broken_url)
    else:
        print("All files downloaded as expected (no broken urls)")