from api_calls import *
import time

sample_query = (
    "https://api.wikimedia.org/core/v1/wikipedia/en/page/Earth/links/language"
)


def pull_data(articles: list, langs: list, timeStart: datetime, timeEnd: datetime):

    num_requests = len(articles) * len(langs)
    progress = 0

    output_dict = {}

    for article in articles:
        l = []
        for language in langs:

            if language != "en":
                try:
                    articleTitle = get_article_name(article, "en", language)
                except:

                    try:
                        s_response = re.get(sample_query).json()
                        if s_response["httpCode"] == 429:
                            print("Sleeping!")
                            time.sleep(3600)
                            articleTitle = get_article_name(article, "en", language)
                        else:
                            v = {}

                    except:
                        v = {}

            else:
                articleTitle = article
            try:
                v = get_num_edits_range(
                    articleTitle=articleTitle,
                    timeStart=timeStart,
                    timeEnd=timeEnd,
                    lang=language,
                )
            except:

                try:
                    s_response = re.get(sample_query).json()
                    if s_response["httpCode"] == 429:
                        print("Sleeping!")
                        time.sleep(3600)
                        v = get_num_edits_range(
                            articleTitle=articleTitle,
                            timeStart=timeStart,
                            timeEnd=timeEnd,
                            lang=language,
                        )

                    else:
                        v = {}
                except:
                    v = {}

            l.append({language: v})

            progress += 1

            print("Progress:")
            print(str(progress) + "/" + str(num_requests))

        output_dict[article] = l

    file_name = "extract_run_" + datetime.now().strftime("%Y-%m-%d-%H-%M")
    with open("data/raw_data/{}.json".format(file_name), "w") as json_file:
        json.dump(output_dict, json_file, indent=4)

    print(output_dict)
    return


if __name__ == "__main__":

    pull_data(
        articles=[
            "Kyiv",
            "Kievan_Rus'",
            "Stepan_Bandera",
            "Bohdan_Khmelnytsky",
            "Cossacks",
            "Ukrainian_language",
            "Holodomor",
            "Borscht",
            "Symon_Petliura",
        ],
        langs=["en", "de", "ru", "uk"],
        timeStart=datetime(2007, 1, 1, 0, 0, 0),
        timeEnd=datetime(2024, 4, 30, 23, 59, 59),
    )
