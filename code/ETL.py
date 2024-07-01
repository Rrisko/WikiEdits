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


    revisions_all= {}
    failed_runs = []
    ct = 0
    tt = len(articles) * len(langs)

    for l in langs:

        revisions_all[l] = {}

        for a in articles:

            print(a)
            
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
        
        revisions_df_l = join_users_edits(lang = lang, revisions = revisions_df_l)

        try: 
            revisions_df = pd.concat([revisions_df, revisions_df_l])
        except:
            revisions_df = revisions_df_l
    
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

    ETL(["Zvolen", "Brezno"], ["en", "sk"], "SK")
