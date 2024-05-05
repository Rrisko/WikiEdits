## Here functions for pulling metadata from wikipedia are defined

import requests as re
import json
import pandas as pd
from datetime import *
import time


def get_num_edits(
    articleTitle: str,
    timeStart: datetime = datetime(2024, 1, 1, 0, 0, 0),
    timeEnd: datetime = datetime(2024, 1, 31, 23, 59, 59),
    lang: str = "en",
) -> int:
    """Gets raw number of edits of an article in the time range"""

    revisionIds = [
        get_revision_id(articleTitle, lang, timeStart, "newer"),
        get_revision_id(articleTitle, lang, timeEnd, "older"),
    ]
    articleTitle = articleTitle.replace(" ", "_")

    query_num_edits = "https://api.wikimedia.org/core/v1/wikipedia/{lang}/page/{title}/history/counts/edits?from={fromRevision}&to={untilRevision}".format(
        lang=lang,
        title=articleTitle,
        fromRevision=revisionIds[0],
        untilRevision=revisionIds[1],
    )

    response = re.get(query_num_edits).json()

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

    response = re.get(query_langs).json()

    return [d for d in response if d.get("code") == LangTwo][0]["title"]


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
    response = re.get(query).json()
    revision_id = response["continue"]["rvcontinue"].split("|", 1)[1]

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
        time.sleep(10)
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

    return num_edits


if __name__ == "__main__":

    print(get_article_name("Vienna"))
    print(get_num_edits("Vienna"))
    print(get_num_edits_range("Vienna"))
