## Here functions for pulling metadata from wikipedia are defined
import re
import requests as req
import json
import pandas as pd
from datetime import *
import time as tm
from collections import Counter


def get_num_edits(
    articleTitle: str,
    timeStart: datetime = datetime(2024, 1, 1, 0, 0, 0),
    timeEnd: datetime = datetime(2024, 1, 31, 23, 59, 59),
    lang: str = "en",
) -> int:
    """Gets raw number of edits of an article in the time range"""

    articleTitle = articleTitle.replace(" ", "_")

    revisionIds = [
        get_revision_id(articleTitle, lang, timeStart, "newer"),
        get_revision_id(articleTitle, lang, timeEnd, "older"),
    ]

    if not all(revisionIds):
        return 0

    query_num_edits = "https://api.wikimedia.org/core/v1/wikipedia/{lang}/page/{title}/history/counts/edits?from={fromRevision}&to={untilRevision}".format(
        lang=lang,
        title=articleTitle,
        fromRevision=revisionIds[0],
        untilRevision=revisionIds[1],
    )

    response = req.get(query_num_edits).json()

    if response["limit"] is True:
        print(articleTitle + " " + timeStart + " " + timeEnd + " " + lang)
        print("Too many edits, cut the time range")
        return

    return response["count"]


def get_article_name(
    articleTitle: str, LangOne: str = "en", LangTwo: str = "de"
) -> str:
    """Returns article's name in LangTwo"""

    articleTitle = articleTitle.replace(" ", "_")

    query_langs = "https://api.wikimedia.org/core/v1/wikipedia/{lang}/page/{title}/links/language".format(
        lang=LangOne, title=articleTitle
    )

    response = req.get(query_langs).json()

    return [d for d in response if d["code"] == LangTwo][0]["title"]


def get_revision_id(
    articleTitle: str,
    lang: str = "en",
    time: datetime = datetime(2024, 1, 1, 0, 0, 0),
    direction: str = "newer",
) -> str:
    """Returns revision id closest to the given datetime"""

    datetimeStr = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    articleTitle = articleTitle.replace(" ", "_")

    query = "https://{lang}.wikipedia.org/w/api.php?action=query&format=json&prop=revisions&titles={title}&rvlimit=1&rvprop=content&rvdir={direction}&rvstart={start}".format(
        lang=lang, title=articleTitle, direction=direction, start=datetimeStr
    )
    response = req.get(query).json()

    try:
        revision_id = response["continue"]["rvcontinue"].split("|", 1)[1]
    except KeyError as e:
        if next(iter(response)) == "batchcomplete":
            return None
        else:

            print("Sleeping!")
            print(query)
            tm.sleep(690)
            get_revision_id(articleTitle, lang, time, direction)

    return revision_id


def get_num_edits_range(
    articleTitle: str,
    timeStart: datetime = datetime(2020, 1, 1, 0, 0, 0),
    timeEnd: datetime = datetime(2024, 4, 30, 23, 59, 59),
    lang: str = "en",
) -> dict:

    num_edits = {}

    first_datetime = datetime(timeStart.year, timeStart.month, 1, 0, 0, 0)

    while first_datetime < timeEnd:
        tm.sleep(3)
        next_month = first_datetime.month % 12 + 1
        next_year = first_datetime.year + (1 if next_month == 1 else 0)
        second_datetime = datetime(next_year, next_month, 1, 0, 0, 0) - timedelta(
            seconds=1
        )

        n_edits = get_num_edits(
            articleTitle=articleTitle,
            timeStart=first_datetime,
            timeEnd=second_datetime,
            lang=lang,
        )
        k = str(first_datetime.year) + "-" + str(first_datetime.month)
        num_edits[k] = n_edits

        first_datetime = second_datetime + timedelta(seconds=1)
        print(k)

    return num_edits


def get_revisions_query(query: str, timeStop: datetime, initial_list=None):

    tm.sleep(3)
    if initial_list == None:
        output_list = []
    else:
        output_list = initial_list

    response = req.get(query).json()

    # try:
    #    code = response["httpCode"]
    #    if code == "404":
    #        return None
    #   if code == "429":
    #        tm.sleep(690)
    #      get_revisions_query(query)

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
    articleTitle: str, lang: str = "en", timeStop: datetime = datetime(2020, 1, 1)
):

    query = "https://api.wikimedia.org/core/v1/wikipedia/{lang}/page/{articleTitle}/history".format(
        lang=lang, articleTitle=articleTitle
    )

    output_list = get_revisions_query(query, timeStop)
    return output_list


def get_revisions_by_month(
    articleTitle: str, lang: str = "en", timeStop: datetime = datetime(2020, 1, 1)
):

    articleTitle = articleTitle.replace(" ", "_")
    revisions = get_revisions(articleTitle, lang, timeStop)
    revisions = [dt.strftime("%Y-%m") for dt in revisions]
    return dict(Counter(revisions))


def convert_log_to_datetime(log_str: str) -> datetime:

    p = r"\(([^\)]+)\)"
    match = re.search(p, log_str).group(1)
    if match == "indefinite":
        return datetime(2029, 12, 31, 23, 59, 59)

    date_string = match[8:-5]

    date_dt = datetime.strptime(date_string, "%H:%M, %d %B %Y")

    return date_dt


def get_protection(article: str, lang: str = "en"):

    output_list = []

    if lang != "en":
        try:
            articleTitle = get_article_name(article, "en", lang)
        except:
            return output_list
    else:
        articleTitle = article

    articleTitle = articleTitle.replace(" ", "_")

    query = "https://{lang}.wikipedia.org/w/api.php?action=query&list=logevents&letype=protect&letitle={articleTitle}&leend=2007-12-31T23:59:59&format=json".format(
        lang=lang, articleTitle=articleTitle
    )

    response = req.get(query).json()

    try:
        protections = response["query"]

        if protections == []:
            return output_list
        for log in protections["logevents"]:

            protection_timestamp = datetime.strptime(
                log["timestamp"], "%Y-%m-%dT%H:%M:%SZ"
            )
            if lang == "en":
                protection_limit = convert_log_to_datetime(log["params"]["description"])
            else:
                protection_limit = log["params"]["details"][0]["expiry"]

                if protection_limit == "infinite":
                    protection_limit = datetime(2029, 12, 31, 23, 59, 59)

                else:
                    protection_limit = datetime.strptime(
                        protection_limit, "%Y-%m-%dT%H:%M:%SZ"
                    )

            output_list.append(
                {
                    "article": article,
                    "language": lang,
                    "start": protection_timestamp,
                    "end": protection_limit,
                }
            )

        return output_list

    except:

        return output_list


if __name__ == "__main__":

    a = get_article_name("Israel", "en", "sk")
    # print(a)
    # print(get_revisions_by_month(a, lang="sk", timeStop=datetime(2023, 11, 1)))
    print(get_protection("Shawarma", "en"))
