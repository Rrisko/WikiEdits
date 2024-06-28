## Here functions for pulling metadata from wikipedia are defined
import re
import requests as req
import json
import pandas as pd
from datetime import *
import time as tm
from collections import Counter
import locale

## Universal helper function

def get_article_name(
    articleTitle: str, LangOne: str = "en", LangTwo: str = "de"
) -> str :
    
    """
    Returns article's name in LangTwo -
    i.e. get_article_name("Vienna", "en", "de") returns "Wien"
    """

    articleTitle = articleTitle.replace(" ", "_")

    query_langs = "https://api.wikimedia.org/core/v1/wikipedia/{lang}/page/{title}/links/language".format(
        lang=LangOne, title=articleTitle
    )

    response = req.get(query_langs).json()

    return [d for d in response if d["code"] == LangTwo][0]["title"]

## Functions to pull and transform revisions

def get_revisions_detailed(
    article : str,
    lang: str = "en",
    prev : str  = None,
    user : str = None) :
    
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
    user : str = None) -> pd.DataFrame :

    revisions = get_revisions_detailed(articleTitle,lang, prev, user)
    return len(revisions)
    
def transform_revisions_detailed(
    articleTitle: str,
    lang: str = "en",
) -> pd.DataFrame:
    
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

## Functions to retrieve and transform user edits

def get_user_edits_count(user : str, lang : str) -> int:

    """Returns count of all edits a user has made on all articles on inputted language version wikipedia"""

    query = "https://{lang}.wikipedia.org/w/api.php?action=query&list=users&usprop=editcount&ususers={user}&format=json".format(user=user, lang=lang)
    response = req.get(query).json()

    try:
        return response['query']['users'][0]['editcount']
    except:
        return 0

def get_proportion_user_edits(users : list, lang : str, article : str) -> pd.DataFrame:

    users_edits = []

    for user in users:
        
        total_edits = get_user_edits_count(user, lang)
        append_dict = {'user': user, 'article': article, 'language' : lang, 'total_edits' : total_edits}
        users_edits.append(append_dict)        

    return pd.DataFrame(users_edits)

def join_users_edits(article : str, lang : str) -> pd.DataFrame:

    revisions = transform_revisions_detailed(article, lang)
    unique_users = list(revisions['user'].unique())

    user_edits = get_proportion_user_edits(unique_users, lang, article)

    merged_df = pd.merge(revisions, user_edits, how = 'left', on = ['user', 'article', 'language'])
    merged_df['article_edits'] = merged_df.groupby('user')['user'].transform('count')

    return merged_df

## And here everything is brought together

def etl_edits_users_detailed(articles : list, langs : list) -> pd.DataFrame:

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

## Functions to pull and transform protection logs

def pull_protections(article : str, lang : str):

    if lang != 'en':
        try:
            article = get_article_name(article, "en", lang)
        except:
            return None
    
    query = 'https://{lang}.wikipedia.org/w/api.php?action=query&list=logevents&letype=protect&letitle={article}&format=json&lelimit=500&leprop=title|type|user|timestamp|comment|details'.format(
        lang=lang, article=article
    )

    response = req.get(query).json()

    try:
        logevents = response['query']['logevents']
        return logevents
    except:
        print(response)
        return None

def pull_multiple_protections(articles : list, langs : list) -> dict :

    output_dict = {}

    for a in articles:
        output_dict[a] = {}

        for l in langs:

            try:
                output_dict[a][l] = pull_protections(a,l)
            except:
                print(pull_protections(a,l))
                output_dict[a][l] = {}

        print(a)
    
    return output_dict

def try_except_extraction(desired_value):

    try:
        return desired_value
    except:
        return None
    
def transform_protections(input_list : list, lang : str) -> pd.DataFrame :

    output_list = []

    # when there are no protection logs
    if input_list is None:
        return output_list
    if input_list == []:
        return output_list
    
    for log in input_list:

        transformed_log = {}
        transformed_log['language'] = lang
        transformed_log['title'] = log['title']
        transformed_log['timestamp'] = datetime.strptime(log["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        try:
            transformed_log['user'] = log['user']
        except:
            print(log)
            transformed_log['user'] = None
        transformed_log['action'] = log['action']
        transformed_log['comment'] = log['comment']
        
        try:
            transformed_log['type'] = [d['type'] for d in log['params']['details']]
        except:
            transformed_log['type'] = ""
        
        try:
            transformed_log['level'] = [d['level'] for d in log['params']['details']]
        except:
            transformed_log['level'] = ""
        
        transformed_log['expiry'] = extract_expiry(log, lang)

        output_list.append(transformed_log)

    return pd.DataFrame(output_list)

def transform_multiple_protections(input_dict : dict):

    for a, d in input_dict.items():
        for l, log in d.items():

            

            df_log = transform_protections(log, l)

            print(a)
            print(l)
            print('###')
            print(df_log)

            if isinstance(df_log, pd.DataFrame):
                
                df_log['article'] = a

                try:

                    output_df = pd.concat([output_df, df_log], ignore_index=True)
                except:
                    output_df = df_log
    
    return output_df

def extract_expiry(log : dict, lang : str):

    # easiest way:
    try:
        expiry_str = [d['expiry'] for d in log['parmas']['details']]
        expiry = [convert_log_to_datetime(e, lang, True) for e in expiry_str]
    except:

        if log['params'] != {} and 'description' in log['params'].keys():
            expiry_str = log['params']['description']
            
        elif 'comment' in log.keys():
            expiry_str = log['comment']
        else:
            print(log)
            return None
        
        try:
            expiry = convert_log_to_datetime(expiry_str, lang, False)
        except:
            print(expiry_str)
            expiry=None
    
    return expiry
    
## Some ad hoc lists and dictionaries

infinite_options = [
    "indefinite",
    "unbeschränkt",
    "безстроково",
    "غير محدد",
    "бессрочно",
    "na neurčito",
    "do odvolání",
    "na zawsze",
    "infinito",
    "infinite"
]

russian_genitive_months = {
    "января": "январь",
    "февраля": "февраль",
    "марта": "март",
    "апреля": "апрель",
    "мая": "май",
    "июня": "июнь",
    "июля": "июль",
    "августа": "август",
    "сентября": "сентябрь",
    "октября": "октябрь",
    "ноября": "ноябрь",
    "декабря": "декабрь",
}

ukrainian_genitive_months = {
    "січня": "Січень",
    "лютого": "Лютий",
    "березня": "Березень",
    "квітня": "Квітень",
    "травня": "Травень",
    "червня": "Червень",
    "липня": "Липень",
    "серпня": "Серпень",
    "вересня": "Вересень",
    "жовтня": "Жовтень",
    "листопада": "Листопад",
    "грудня": "Грудень",
}

arabic_to_english_months = {
    "يناير": "January",
    "فبراير": "February",
    "مارس": "March",
    "أبريل": "April",
    "مايو": "May",
    "يونيو": "June",
    "يوليو": "July",
    "أغسطس": "August",
    "سبتمبر": "September",
    "أكتوبر": "October",
    "نوفمبر": "November",
    "ديسمبر": "December",
}


def convert_log_to_datetime(log_str: str, lang : str = "en", from_details : bool = False) -> datetime:
    
    """
    Helper function to retrieve datetime limits from protection logs
    """

    if from_details: # converts ['params']['details']
        if log_str == "infinite":
            return datetime(2029, 12, 31, 23, 59, 59)

        else:
            try:
                return datetime.strptime(
                    log_str, "%Y-%m-%dT%H:%M:%SZ"
                )
            except:
                print(log_str)
                return None
        
    format_time = "%H:%M, %d %B %Y"

    if lang == "en":
        locale.setlocale(locale.LC_ALL, "en_US")
        log_str = log_str.replace("expires ", "")

    elif lang == "de":
        locale.setlocale(locale.LC_ALL, "de_DE")
        log_str = log_str.replace("bis ", "")
        log_str = log_str.replace(" Uhr", "")

        format_time = "%d. %B %Y, %H:%M"

    elif lang == "ru":
        locale.setlocale(locale.LC_ALL, "ru_RU")
        log_str = log_str.replace("истекает ", "")

        log_str = re.sub(
            r"\b" + r"|\b".join(russian_genitive_months) + r"\b",
            lambda m: russian_genitive_months.get(m.group(), m.group()),
            log_str,
        )

    elif lang == "uk":
        locale.setlocale(locale.LC_ALL, "uk_UA")
        log_str = log_str.replace("закінчується ", "")

        log_str = re.sub(
            r"\b" + r"|\b".join(ukrainian_genitive_months) + r"\b",
            lambda m: ukrainian_genitive_months.get(m.group(), m.group()),
            log_str,
        )

    elif lang == "ar":
        locale.setlocale(locale.LC_ALL, "ar")
        log_str = log_str.replace("تنتهي في ", "")
        log_str = log_str.replace("،", ",")
        log_str = re.sub(
            r"\b" + r"|\b".join(arabic_to_english_months) + r"\b",
            lambda m: arabic_to_english_months.get(m.group(), m.group()),
            log_str,
        )
        locale.setlocale(locale.LC_ALL, "en_US")
        format_time = "%H:%M, %d %b %Y"

    elif lang == "sk":
        locale.setlocale(locale.LC_ALL, "sk")
        log_str = log_str.replace("vyprší o ", "")
        format_time = "%H:%M, %d. %B %Y"

    elif lang == "pl":
        locale.setlocale(locale.LC_ALL, "pl")
        log_str = log_str.replace("wygasa ", "")
        format_time = "%H:%M, %d %b %Y"

    elif lang == "cs":
        locale.setlocale(locale.LC_ALL, "cs")
        log_str = log_str.replace("vyprší v ", "")
        format_time = "%d. %m. %Y, %H:%M"

    elif lang == "it":
        locale.setlocale(locale.LC_ALL, "it")
        log_str = log_str.replace("scade il ", "")
        log_str = log_str.replace("scade ", "")
        log_str = log_str.replace("alle ", "")
        format_time = "%d %b %Y %H:%M"

    p = r"\(([^\)]+)\)"
    match = re.search(p, log_str).group(1)

    if (
        match == "indefinite"
        or match == "unbeschränkt"
        or match == "безстроково"
        or match == "غير محدد"
        or match == "бессрочно"
        or match == "na neurčito"
        or match == "do odvolání"
        or match == "na zawsze"
        or match == "infinito"
    ):
        return datetime(2029, 12, 31, 23, 59, 59)

    match = match.replace(" (UTC", "")

    date_string = match  # [8:-5]  # remove expires and UTC substrings

    format_times = ["%H:%M, %d %B %Y", format_time, "%H:%M, %d %b %Y"]
    for f in format_times:
        try:
            date_dt = datetime.strptime(date_string, f)
            break
        except:
            next

    locale.setlocale(locale.LC_ALL, "en_US")

    return date_dt


def etl_protections(articles: list, langs: list, filename : str = "") -> pd.DataFrame:

    raw_data = pull_multiple_protections(articles, langs)

    with open("data/raw_data/raw_protections_{f}_{d}.json".format(
        f = filename, d = datetime.now().strftime("%Y-%m-%d-%H-%M")), "w") as json_file:
        
        json.dump(raw_data, json_file, indent=4)
    
    transformed_data = transform_multiple_protections(raw_data)

    transformed_data.to_csv('data/protection_data/protections_{f}_{d}.csv'.format(
        f = filename, 
        d = datetime.now().strftime("%Y-%m-%d-%H-%M"))
    )

    return transformed_data

if __name__ == "__main__":

## would retrieve data for 3x3 articles

    sk_articles = [
        "Zvolen", "Brezno", "Prievidza"
    ]

    sk_langs = ['en', 'de', 'sk']
    etl_edits_users_detailed(sk_articles, sk_langs)
    etl_protections(sk_articles, sk_langs, "SK")