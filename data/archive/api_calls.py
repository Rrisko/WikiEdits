## Here functions for pulling metadata from wikipedia are defined
import re
import requests as req
import json
import pandas as pd
from datetime import *
import time as tm
from collections import Counter
import locale


def get_article_name(
    articleTitle: str, LangOne: str = "en", LangTwo: str = "de"
) -> str:
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


def get_revisions_query(query: str, timeStop: datetime, initial_list=None) -> list:
    """
    Returns list of timestamps when revisions on an article where made
    """

    tm.sleep(2)  # to avoid http 429 code too many requests

    if initial_list == None:
        output_list = []
    else:
        output_list = initial_list

    response = req.get(query).json()

    revisions = response["revisions"]
    for d in revisions:
        ts = d["timestamp"]

        output_list.append(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ"))

    oldest = output_list[-1]
    if oldest < timeStop:
        return output_list

    if "older" in list(response.keys()):
        return get_revisions_query(response["older"], timeStop, output_list)
    else:
        return output_list

def get_revisions_query_detailed(query: str, timeStop: datetime, initial_list=None) -> list:
    """
    Returns list of timestamps when revisions on an article where made
    """

    tm.sleep(2)  # to avoid http 429 code too many requests

    if initial_list == None:
        output_list = []
    else:
        output_list = initial_list

    response = req.get(query).json()

    revisions = response["revisions"]
    for d in revisions:
        ts = d["timestamp"]

        output_list.append(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ"))

    oldest = output_list[-1]
    if oldest < timeStop:
        return output_list

    if "older" in list(response.keys()):
        return get_revisions_query(response["older"], timeStop, output_list)
    else:
        return output_list


def get_revisions(
    articleTitle: str,
    lang: str = "en",
    timeStop: datetime = datetime(2020, 1, 1),
    f: str = "",
) -> list:
    """
    Based on inputted article and language creates a request, calls get_revisions_query() and returns list of timestamps when revisions on an article where made
    """

    if f != "":
        f = "?filter=" + f

    query = "https://api.wikimedia.org/core/v1/wikipedia/{lang}/page/{articleTitle}/history{f}".format(
        lang=lang, articleTitle=articleTitle, f=f
    )

    output_list = get_revisions_query(query, timeStop)
    return output_list


def get_revisions_by_month(
    articleTitle: str,
    lang: str = "en",
    timeStop: datetime = datetime(2020, 1, 1),
    f: str = "",
) -> dict:
    """Calls get_revisions().
    The output list is grouped by month and transformed to a dictionary in format {'month1': count1, 'month2': count2, ...}
    """

    articleTitle = articleTitle.replace(" ", "_")  # when article name has spaces in it

    revisions = get_revisions(articleTitle, lang, timeStop, f)

    revisions = [dt.strftime("%Y-%m") for dt in revisions]
    return dict(Counter(revisions))


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


def convert_log_to_datetime(log_str: str, lang="en") -> datetime:
    """
    Helper function to retrieve datetime limits from protection logs
    """

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


def get_protection_response(article: str, lang: str = "en") -> dict:
    """Requests protection log of an article, if exists, returns response"""

    if lang != "en":
        try:
            articleTitle = get_article_name(article, "en", lang)
        except:
            return None
    else:
        articleTitle = article

    articleTitle = articleTitle.replace(" ", "_")

    query = "https://{lang}.wikipedia.org/w/api.php?action=query&list=logevents&letype=protect&letitle={articleTitle}&leend=2007-12-31T23:59:59&format=json".format(
        lang=lang, articleTitle=articleTitle
    )

    try:
        response = req.get(query).json()
        return response["query"]
    except:
        return None


def get_protection(article: str, lang: str = "en") -> list:
    """Queries logs of an article to get all changes in protection status."""

    output_list = []

    protections = get_protection_response(article, lang)

    if protections is None:
        return output_list  # no protection log

    if protections == []:
        return output_list  # no protection log

    for log in protections["logevents"]:
        print(log)
        protection_timestamp = datetime.strptime(log["timestamp"], "%Y-%m-%dT%H:%M:%SZ")

        if lang == "en":
            try:
                if log["params"] != {}:
                    protection_limit = convert_log_to_datetime(
                        log["params"]["description"]
                    )  # need to retrieve substring
                elif log["comment"] != {}:
                    protection_limit = convert_log_to_datetime(
                        log["comment"]
                    )  # need to retrieve substring
            except:
                continue

        else:
            try:
                protection_limit = log["params"]["details"][0][
                    "expiry"
                ]  # found in the json structure

                if protection_limit == "infinite":
                    protection_limit = datetime(2029, 12, 31, 23, 59, 59)

                else:
                    protection_limit = datetime.strptime(
                        protection_limit, "%Y-%m-%dT%H:%M:%SZ"
                    )

            except KeyError as k:
                if "description" in log["params"].keys():

                    protection_limit = convert_log_to_datetime(
                        log["params"]["description"], lang
                    )

                else:
                    next

        output_list.append(
            {
                "article": article,
                "language": lang,
                "start": protection_timestamp,
                "end": protection_limit,
            }
        )

    return output_list


def get_user_edits(user: str, lang: str, prev: str = None):

    uccontinue = ""

    if prev is not None:
        uccontinue += "uccontinue={prev}&".format(prev=prev)

    query = "https://{lang}.wikipedia.org/w/api.php?action=query&list=usercontribs&ucuser={user}&uclimit=500&{uccontinue}format=json".format(
        lang=lang, user=user, uccontinue=uccontinue
    )

    response = req.get(query).json()
    contribs = response["query"]["usercontribs"]

    try:
        prev = response["continue"]["uccontinue"]
    except:
        prev = ""
    
    if prev == "":
        return contribs
    
    else:
        contribs.extend(get_user_edits(user, lang, prev))
        return contribs


if __name__ == "__main__":

    # a = get_article_name("Israel", "en", "sk")
    # print(a)
    # print(get_revisions_by_month(a, lang="sk", timeStop=datetime(2023, 11, 1)))
    # print(get_protection("Shawarma", "en"))
    print(get_user_edits("Serapolous", "en"))

