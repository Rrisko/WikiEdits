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
    articleTitle  : str,
    lang: str = "en",
    prev : str  = None,
    user : str = None):
    """
    Based on inputted article and language creates a request, calls get_revisions_query() and returns list of timestamps when revisions on an article where made
    """

    if lang != "en":
        articleTitle = get_article_name(articleTitle, "en", lang)

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
        revisions.extend(get_revisions_detailed(articleTitle, lang, prev))
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
    
    if lang != "en":
        article = get_article_name(article, "en", lang)

    users_edits = []

    print(len(users))
    ct = 0

    for user in users:
        
        total_edits = get_user_edits_count(user, lang)
        append_dict = {'user': user, 'article': article, 'language' : lang, 'total_edits' : total_edits}
        users_edits.append(append_dict)
        print(ct)
        if ct % 25 == 0: print(ct)

    return pd.DataFrame(users_edits)

def join_users_edits(article : str, lang : str):

    revisions = transform_revisions_detailed(article, lang)
    unique_users = list(revisions['user'].unique())

    user_edits = get_proportion_user_edits(unique_users, lang, article)

    merged_df = pd.merge(revisions, user_edits, how = 'left', on = ['user', 'article', 'language'])
    merged_df['article_edits'] = merged_df.groupby('user')['user'].transform('count')

    return merged_df

if __name__ == "__main__":

    print(get_proportion_user_edits(['IamNotU'], 'en', 'Hummus'))