import re
import requests as req
import json
import pandas as pd
from datetime import *
import time as tm
from collections import Counter
import locale
from api_functions import *

def extract_all(articles : list, langs : list, topic : str = "") -> tuple[dict, dict] :

    """Extracts, stores and returns raw data for both edits and protection history"""

    revisions_all= {}
    failed_runs = []
    ct = 0
    tt = len(articles) * len(langs)

    for l in langs:

        revisions_all[l] = {}

        for a in articles:
            
            print("Progress {run}/{total}".format(run = ct, total = tt))

            try:
                revisions = get_revisions_detailed(a, l)
                revisions_all[l][a] = revisions
                tm.sleep(10)
                
            except:
                print("Failed fetching data for {l}/{a}".format(l=l, a=a))
                failed_runs.append((l,a))
            
            ct+=1
    
    print("Failed runs: ")
    print(failed_runs)
    
    with open("data/raw_data/edits/raw_edits_{f}_{d}.json".format(
        f = topic, d = datetime.now().strftime("%Y-%m-%d-%H-%M")), "w") as json_file:
        json.dump(revisions_all, json_file, indent=4)

    raw_protections = pull_multiple_protections(articles, langs)
    
    with open("data/raw_data/raw_protections_{f}_{d}.json".format(
        f = topic, d = datetime.now().strftime("%Y-%m-%d-%H-%M")), "w") as json_file:
        
        json.dump(raw_protections, json_file, indent=4)
    
    return revisions_all, raw_protections

def ETL(articles : list, langs : list, topic : str = ""):

    """Extracts raw data, then applies transformations, then stores for both edit and protection history."""

    ## EXTRACT
    raw_revisions, raw_protections = extract_all(articles, langs, topic)

    print("Extraction complete")
    
    ## TRANSFORM
    for lang, lang_dict in raw_revisions.items():

        for article, edit_list in lang_dict.items():

            append_df = transform_revisions_detailed(article, lang, edit_list)
            append_df['article_edits'] = append_df.groupby('user')['user'].transform('count')
            
            try: 
                revisions_df_l = pd.concat([revisions_df_l, append_df])
            except:
                revisions_df_l = append_df

        tm.sleep(1200)
        
        revisions_df_l = join_users_edits(lang = lang, revisions = revisions_df_l)

        try: 
            revisions_df = pd.concat([revisions_df, revisions_df_l])
        except:
            revisions_df = revisions_df_l
        
        del revisions_df_l
    
    protections_df = transform_multiple_protections(raw_protections)

    print("Transformation complete")
    
    ## LOAD
    revisions_df.to_csv("data/detailed_data/detailedEdits_{f}_{t}.csv".format(
        f = topic, 
        t = datetime.now().strftime("%Y-%m-%d-%H-%M")))
    
    protections_df.to_csv('data/protection_data/protections_{f}_{d}.csv'.format(
        f = topic, 
        d = datetime.now().strftime("%Y-%m-%d-%H-%M"))
    )

    print("Data stored")

    return revisions_df, protections_df


if __name__ == "__main__":

    us_articles = [
       'Ulysses_S._Grant', "Sherman's_March_to_the_Sea",
       'William_Tecumseh_Sherman', 'Union_Army',
       'Confederate_States_Army', 'Robert_E._Lee', 'Joseph_E._Johnston',
       'Alexander_H._Stephens', 'James_Longstreet',
       'United_Daughters_of_the_Confederacy', 'Army_of_Northern_Virginia',
       'Jefferson_Davis', 'Origins_of_the_American_Civil_War',
       'Confederate_States_of_America', 'Abraham_Lincoln',
       'Battle_of_Gettysburg', 'Judah_P._Benjamin',
       'John_C._Breckinridge', 'Joseph_Wheeler', 'P._G._T._Beauregard',
       'Franklin_Buchanan', 'Nathan_Bedford_Forrest', 'Ku_Klux_Klan',
       'John_C._Frémont', 'Joseph_Hooker', 'George_Meade',
       'Wilmington_massacre', 'Red_Shirts_(United_States)',
       'United_Confederate_Veterans', 'Confederate_History_Month',
       'Robert_E._Lee_Day', 'Stonewall_Jackson', 'Jim_Crow_laws',
       'John_Brown_(abolitionist)', 'William_Lloyd_Garrison',
       'Frederick_Douglass', 'Thaddeus_Stevens',
       'Battle_of_the_Wilderness', 'Battle_of_Antietam',
       'Emancipation_Proclamation',
       'Thirteenth_Amendment_to_the_United_States_Constitution',
       'Slavery_in_the_United_States', "States'_rights",
       'Historiographic_issues_about_the_American_Civil_War',
       'Reconstruction_era'
    ]

    us_langs = ['en', 'de']

    ip_articles = [
       'Nakba', 'Mandatory_Palestine', '1948_Arab-Israeli_War',
       'David_Ben-Gurion', 'Yasser_Arafat', 'Six-Day_War',
       'Yom_Kippur_War', 'Hummus', 'Falafel', 'Shawarma',
       'First_Intifada', 'United_Nations_Partition_Plan_for_Palestine',
       'Intercommunal_conflict_in_Mandatory_Palestine',
       'Lehi_(militant_group)', 'Irgun', "Ze'ev_Jabotinsky", 'Haganah',
       '1947–1948_civil_war_in_Mandatory_Palestine',
       '1948_Arab–Israeli_War', 'Yitzhak_Rabin', 'Palmach', 'Moshe_Dayan',
       'Jewish_exodus_from_the_Muslim_world',
       '1936–1939_Arab_revolt_in_Palestine', 'Amin_al-Husseini',
       '1948_Palestinian_expulsion_and_flight',
       'List_of_towns_and_villages_depopulated_during_the_1947–1949_Palestine_war',
       'Plan_Dalet', 'Abd_al-Qadir_al-Husayni', '1929_Hebron_massacre',
       'Causes_of_the_1948_Palestinian_expulsion_and_flight',
       'Deir_Yassin_massacre', 'Menachem_Begin', 'Kfar_Etzion_massacre',
       'Hebrew_language', 'Suez_Crisis', 'Egypt–Israel_peace_treaty',
       'Palestinian_Arabic', 'Culture_of_Palestine',
       'Palestinian_cuisine', 'Samih_al-Qasim', 'Mahmoud_Darwish',
       'Origin_of_the_Palestinians'
    ]

    ip_langs = ['en', 'de', 'ar']

    ua_articles = [
       'Kyiv', "Kievan_Rus'", 'Stepan_Bandera', 'Bohdan_Khmelnytsky',
       'Cossacks', 'Ukrainian_language', 'Holodomor', 'Borscht',
       'Symon_Petliura', "Ukrainian_People's_Republic",
       'Mykhailo_Hrushevsky', 'Ukrainian_literature', 'Ivan_Franko',
       'Ukrainian_Insurgent_Army', 'Pampushka', 'Syrniki', 'Rusyns',
       'Vyshyvanka', 'Ukrainian_Soviet_Socialist_Republic',
       'Pereiaslav_Agreement', "West_Ukrainian_People's_Republic",
       'Massacres_of_Poles_in_Volhynia_and_Eastern_Galicia',
       'Orange_Revolution', 'Ukrainian_War_of_Independence',
       'Principality_of_Kiev', 'Kyiv_Pechersk_Lavra', 'Golden_Gate,_Kyiv',
       'Bakhchysarai_Palace', 'Khreshchatyk',
       'Kamianets-Podilskyi_Castle', 'Saint_Sophia_Cathedral,_Kyiv',
       'Kobzar', 'Hryhorii_Skovoroda', 'Lesya_Ukrainka', "Rus'_people",
       'Zaporozhian_Cossacks', 'Khmelnytsky_Uprising', 'Nikolai_Gogol',
       'Taras_Shevchenko', 'Organisation_of_Ukrainian_Nationalists',
       'Pierogi', 'Kolach_(bread)', 'Paska_(bread)']

    ua_langs = ['en', 'de', 'uk', 'ru']

    ETL(us_articles, us_langs, "US_Civil_War")
    ETL(ip_articles, ip_langs, "Israel_Palestine")
    ETL(ua_articles, ua_langs, "Ukraine")
