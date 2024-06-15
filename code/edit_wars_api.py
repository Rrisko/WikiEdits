## Here functions for pulling metadata from wikipedia are defined
import re
import requests as req
import json
import pandas as pd
from datetime import *
import time as tm
from collections import Counter
import locale
from api_calls import *

def get_revisions_detailed(
    article : str,
    lang: str = "en",
    prev : str  = None,
    user : str = None):
    """
    Based on inputted article and language creates a request, calls get_revisions_query() and returns list of timestamps when revisions on an article where made
    """

    if lang != "en":
        articleTitle = get_article_name(article, "en", lang)
    else:
        articleTitle = article

    rvcontinue = ""
    rvuser = ""

    if prev is not None:
        rvcontinue += "rvcontinue={prev}&".format(prev=prev)

    if user is not None:
        rvuser += "rvuser={user}&".format(user=user)

    query = "https://{lang}.wikipedia.org/w/api.php?action=query&prop=revisions&rvlimit=500&titles={articleTitle}&{rvcontinue}{rvuser}format=json&rvprop=ids|timestamp|flags|comment|user|tags|size".format(
        lang=lang, articleTitle=articleTitle, rvcontinue = rvcontinue, rvuser=rvuser
    )

    response = req.get(query).json()
    #print(response)
    revisions = response["query"]["pages"]
    revisions = next(iter(revisions.values()))['revisions']

    try:
        prev = response["continue"]["rvcontinue"]
    except:
        prev = ""
    
    if prev == "":
        return revisions
    
    else:
        revisions.extend(get_revisions_detailed(article, lang, prev))
        return revisions
    
def count_revisions(articleTitle  : str,
    lang: str = "en",
    prev : str  = None,
    user : str = None):

    revisions = get_revisions_detailed(articleTitle,lang, prev, user)
    return len(revisions)
    
def transform_revisions_detailed(
        
    articleTitle: str,
    lang: str = "en",
):
    raw_data = get_revisions_detailed(articleTitle, lang)
    df_list = []
    for r in raw_data:

        r['reverted'] = 1 if 'mw-reverted' in r['tags'] else 0
        r['reversion'] = 1 if ('mw-undo' in r['tags'] )| ('mw-manual-revert' in r['tags']) else 0
        r['article'] = articleTitle
        r['language'] = lang
        r['timestamp'] = datetime.strptime(r['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
        r = {key: value for key, value in r.items() if key in ['article', 'language', 'user', 'timestamp', 'size', 'reverted', 'reversion']}
        df_list.append(r)
    

    return pd.DataFrame(df_list)

def get_user_edits_count(user : str, lang : str):

    query = "https://{lang}.wikipedia.org/w/api.php?action=query&list=users&usprop=editcount&ususers={user}&format=json".format(user=user, lang=lang)
    response = req.get(query).json()

    try:
        return response['query']['users'][0]['editcount']
    except:
        return 0
    
def transform_user_edits(user : str, lang : str):

    raw_data = get_user_edits(user, lang)
    
    df_list = []
    for r in raw_data:

       
        r['language'] = lang
        r['timestamp'] = datetime.strptime(r['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
        r = {key: value for key, value in r.items() if key in ['user', 'language', 'title', 'timestamp']}
        df_list.append(r)
    
    print(len(df_list))
    return pd.DataFrame(df_list)

def get_proportion_user_edits(users : list, lang : str, article : str):

    users_edits = []

    

    for user in users:
        
        total_edits = get_user_edits_count(user, lang)
        append_dict = {'user': user, 'article': article, 'language' : lang, 'total_edits' : total_edits}
        users_edits.append(append_dict)
        
        

    return pd.DataFrame(users_edits)

def join_users_edits(article : str, lang : str):

    revisions = transform_revisions_detailed(article, lang)
    unique_users = list(revisions['user'].unique())

    user_edits = get_proportion_user_edits(unique_users, lang, article)

    merged_df = pd.merge(revisions, user_edits, how = 'left', on = ['user', 'article', 'language'])
    merged_df['article_edits'] = merged_df.groupby('user')['user'].transform('count')

    return merged_df

def etl_edits_users_detailed(articles : list, langs : list):

    filepath = "data/detailed_data/detailedEdits_{t}.csv".format(t = datetime.now().strftime("%Y-%m-%d-%H-%M"))

    output_df = pd.DataFrame(columns=['user', 'timestamp', 'size', 'reverted', 'reversion', 'article', 'language', 'total_edits', 'article_edits'])
    
    ct = 0
    tt = len(articles) * len(langs)
    failed_runs = []
    for l in langs:
        for a in articles:
            
            print("Progress {run}/{total}".format(run = ct, total = tt))

            try:
                append_df = join_users_edits(a, l)
                print("Successfully fetched data {l}/{a}".format(l=l, a=a))
                output_df = pd.concat([output_df, append_df], ignore_index=True)
                print("Appended dataframe")
                output_df.to_csv(filepath)
                tm.sleep(10)
                
            except:
                print("Failed fetching data for {l}/{a}".format(l=l, a=a))
                failed_runs.append((l,a))
            
            ct += 1
            
    
    print(failed_runs)
    return output_df
            



if __name__ == "__main__":
    #print(join_users_edits('Hebrew_language', 'de'))
    

    articles = ['Reconstruction_era',
                'Nikolai_Gogol',
                'Taras_Shevchenko',
                'Organisation_of_Ukrainian_Nationalists',
                'Pierogi',
                'Kolach_(bread)',
                'Paska_(bread)'
    ]

    etl_edits_users_detailed(articles, ['en'])

    articles = ['United_Daughters_of_the_Confederacy',
        'Origins_of_the_American_Civil_War',
        'United_Confederate_Veterans',
        'Confederate_History_Month',
        'Robert_E._Lee_Day',
        "States'_rights",
        'Historiographic_issues_about_the_American_Civil_War',
        'Pampushka',
        'Ukrainian_War_of_Independence',
        '1948_Arab-Israeli_War',
        'Intercommunal_conflict_in_Mandatory_Palestine',
        '1947–1948_civil_war_in_Mandatory_Palestine',
        '1948_Arab–Israeli_War',
        'Causes_of_the_1948_Palestinian_expulsion_and_flight',
        'Kfar_Etzion_massacre',
        'Culture_of_Palestine',
        'Samih_al-Qasim',
        'Origin_of_the_Palestinians']

    etl_edits_users_detailed(articles, ['de'])

    articles = ['United_Daughters_of_the_Confederacy',
        'Origins_of_the_American_Civil_War',
        'United_Confederate_Veterans',
        'Confederate_History_Month',
        'Robert_E._Lee_Day',
        "States'_rights",
        'Historiographic_issues_about_the_American_Civil_War',
        'Pampushka',
        'Ukrainian_War_of_Independence',
        '1948_Arab-Israeli_War',
        'Intercommunal_conflict_in_Mandatory_Palestine',
        '1947–1948_civil_war_in_Mandatory_Palestine',
        '1948_Arab–Israeli_War',
        'Causes_of_the_1948_Palestinian_expulsion_and_flight',
        'Kfar_Etzion_massacre',
        'Culture_of_Palestine',
        'Samih_al-Qasim',
        'Origin_of_the_Palestinians']

    etl_edits_users_detailed(articles, ['de'])

    articles = ['1948_Arab-Israeli_War',
        'Culture_of_Palestine',
        'Palestinian_cuisine',
        'Samih_al-Qasim',
        'Mahmoud_Darwish',
        'Origin_of_the_Palestinians']
    
    etl_edits_users_detailed(articles, ['ar'])

    etl_edits_users_detailed(['Holodomor'], ['ru'])

if 1==2:

    israel_palestine_articles = [
        "Nakba","Mandatory_Palestine","1948_Arab-Israeli_War","David_Ben-Gurion","Yasser_Arafat","Six-Day_War","Yom_Kippur_War","Hummus","Falafel","Shawarma","First_Intifada",
        "United_Nations_Partition_Plan_for_Palestine", "Intercommunal_conflict_in_Mandatory_Palestine", "Lehi_(militant_group)", "Irgun", "Ze'ev_Jabotinsky",
        "Haganah", "1947–1948_civil_war_in_Mandatory_Palestine", "1948_Arab–Israeli_War", "Yitzhak_Rabin", "Palmach", "Moshe_Dayan", "Jewish_exodus_from_the_Muslim_world", 
        "1936–1939_Arab_revolt_in_Palestine", "Amin_al-Husseini", "1948_Palestinian_expulsion_and_flight", "List_of_towns_and_villages_depopulated_during_the_1947–1949_Palestine_war", "Plan_Dalet", 
        "Abd_al-Qadir_al-Husayni", "1929_Hebron_massacre", "Causes_of_the_1948_Palestinian_expulsion_and_flight", "Deir_Yassin_massacre", "Menachem_Begin", "Kfar_Etzion_massacre", "Hebrew_language", 
        "Suez_Crisis", "Six-Day_War", "Egypt–Israel_peace_treaty", "Palestinian_Arabic", "Culture_of_Palestine", "Palestinian_cuisine", "Samih_al-Qasim", "Mahmoud_Darwish", "Origin_of_the_Palestinians"
    ]

    israel_palestine_langs = ['en', 'de', 'ar']

    etl_edits_users_detailed(israel_palestine_articles, israel_palestine_langs)

    us_civil_war_articles = [
        "Ulysses_S._Grant","Sherman's_March_to_the_Sea","William_Tecumseh_Sherman","Union_Army","Confederate_States_Army","Robert_E._Lee","Joseph_E._Johnston","Alexander_H._Stephens","James_Longstreet",
        "United_Daughters_of_the_Confederacy","Army_of_Northern_Virginia","Jefferson_Davis","Origins_of_the_American_Civil_War","Confederate_States_of_America","Abraham_Lincoln","Battle_of_Gettysburg", 
        "Judah_P._Benjamin", "John_C._Breckinridge", "Joseph_Wheeler", "P._G._T._Beauregard", "Franklin_Buchanan", "Nathan_Bedford_Forrest", "Ku_Klux_Klan", "John_C._Frémont", "Joseph_Hooker", "George_Meade", 
        "Wilmington_massacre", "Red_Shirts_(United_States)", "United_Confederate_Veterans", "Confederate_History_Month", "Robert_E._Lee_Day", "Stonewall_Jackson", "Jim_Crow_laws", "John_Brown_(abolitionist)", 
        "William_Lloyd_Garrison", "Frederick_Douglass", "Thaddeus_Stevens", "Battle_of_the_Wilderness", "Battle_of_Antietam", "Reconstruction_era", "Emancipation_Proclamation", 
        "Thirteenth_Amendment_to_the_United_States_Constitution", "Slavery_in_the_United_States", "States'_rights", "Historiographic_issues_about_the_American_Civil_War"
    ]

    us_civil_war_langs = ['en', 'de']
    
    etl_edits_users_detailed(us_civil_war_articles, us_civil_war_langs)

    ukraine_articles = [
        "Kyiv","Kievan_Rus'","Stepan_Bandera","Bohdan_Khmelnytsky","Cossacks","Ukrainian_language","Holodomor","Borscht","Symon_Petliura",
        "Ukrainian_People's_Republic","Mykhailo_Hrushevsky","Nikolai_Gogol","Taras_Shevchenko","Ukrainian_literature","Ivan_Franko","Ukrainian_Insurgent_Army","Organisation_of_Ukrainian_Nationalists","Pierogi",
        "Kolach_(bread)", "Paska_(bread)", "Pampushka", "Syrniki", "Rusyns", "Vyshyvanka", "Ukrainian_Soviet_Socialist_Republic", "Pereiaslav_Agreement", "West_Ukrainian_People's_Republic", 
        "Massacres_of_Poles_in_Volhynia_and_Eastern_Galicia", "Orange_Revolution", "Ukrainian_War_of_Independence", "Principality_of_Kiev", "Kyiv_Pechersk_Lavra","Golden_Gate,_Kyiv", "Bakhchysarai_Palace", 
        "Khreshchatyk", "Kamianets-Podilskyi_Castle",  "Saint_Sophia_Cathedral,_Kyiv", "Kobzar", "Hryhorii_Skovoroda", "Lesya_Ukrainka", "Rus'_people", "Zaporozhian_Cossacks", "Khmelnytsky_Uprising"
    ]

    ukraine_langs = ['en', 'de', 'uk', 'ru']

    etl_edits_users_detailed(ukraine_articles, ukraine_langs)