from api_calls import *
import time

sample_query = (
    "https://api.wikimedia.org/core/v1/wikipedia/en/page/Earth/links/language"
)


def pull_data(articles: list, langs: list, timeEnd: datetime):

    num_requests = len(articles) * len(langs)
    progress = 0

    output_dict = {}

    for article in articles:
        l = {}
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
                v = get_revisions_by_month(
                    articleTitle=articleTitle,
                    timeStop=timeEnd,
                    lang=language,
                )
            except:

                try:
                    s_response = re.get(sample_query).json()
                    if s_response["httpCode"] == 429:
                        print("Sleeping!")
                        time.sleep(3600)
                        v = get_revisions_by_month(
                            articleTitle=articleTitle,
                            timeStop=timeEnd,
                            lang=language,
                        )

                    else:
                        v = {}
                except:
                    v = {}

            l[language] = v

            progress += 1

            print("Progress:")
            print(str(progress) + "/" + str(num_requests))

        output_dict[article] = l

    file_name = "extract_run_" + datetime.now().strftime("%Y-%m-%d-%H-%M")
    with open("data/raw_data/{}.json".format(file_name), "w") as json_file:
        json.dump(output_dict, json_file, indent=4)

    return output_dict


def flatten_dict(input_dict: dict) -> pd.DataFrame:

    output_list = []
    for a, v in input_dict.items():
        for l, w in v.items():

            if w == {}:
                output_list.append(
                    {"Article": a, "Language": l, "Month": None, "Count": None}
                )
            else:
                for m, x in w.items():
                    output_list.append(
                        {"Article": a, "Language": l, "Month": m, "Count": x}
                    )

    output_df = pd.DataFrame(output_list)

    file_name = "extract_run_" + datetime.now().strftime("%Y-%m-%d-%H-%M")
    output_df.to_csv("data/transformed_data/{}.csv".format(file_name))

    return output_df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Define expected column names and their data types
    expected_columns = ["Article", "Language", "Month", "Count"]
    expected_types = {
        "Article": str,
        "Language": str,
        "Month": str,
        "Count": (int, float),
    }

    if not set(expected_columns).issubset(list(df.columns)):
        raise ValueError("DataFrame is missing one or more expected columns")

    if df["Article"].isnull().any() or df["Language"].isnull().any():
        raise ValueError(f"Column '{col}' cannot have null values")

    df = df.dropna()

    # Check column data types
    for col in expected_columns:

        if not all(isinstance(val, expected_types[col]) for val in df[col]):
            raise TypeError(f"Column '{col}' does not have the expected data type")

    if not all(len(val) in [2, 3] for val in df["Language"]):
        raise ValueError(
            "Column 'Language' should contain strings with two or three characters"
        )

    if not all(len(val) in [6, 7] for val in df["Month"]):
        raise ValueError(
            "Column 'Month' should contain strings with six or seven characters"
        )

    return df


def append_table(filepath: str, table2: pd.DataFrame):

    try:
        table1 = pd.read_csv(filepath)
        table2 = clean_data(table2)

        concat_table = pd.concat([table1, table2], axis=0)
    except (ValueError, TypeError) as e:
        print("Table2 not clean")
        return
    except FileNotFoundError as e:
        concat_table = clean_data(table2)
    except Exception:
        print("IDK what happened")
        return

    duplicates = concat_table.duplicated(
        subset=["Article", "Language", "Month"], keep=False
    )

    if duplicates.any():
        print("Duplicates!")
    else:
        print("No duplicates found.")

    concat_table.to_csv(filepath)
    return


def ETL(articles: list, langs: list, timeEnd: datetime):

    raw_data = pull_data(articles, langs, timeEnd)
    flattened_data = flatten_dict(raw_data)

    try:
        final_data = clean_data(flattened_data)
        append_table("data/all_data/cleanWiki.csv", final_data)
        print("All good!")
    except:
        print("Failed data load")

    return


if __name__ == "__main__":

    ETL(
        articles=["Holodomor"],
        langs=["en", "de", "ru", "uk"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )
