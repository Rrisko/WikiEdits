from api_calls import *
import time


def pull_data(articles: list, langs: list, timeEnd: datetime, f: str = "") -> dict:
    """
    Calls get_revisions_by_month on each article and language combination,
    collects outputs in output_dict, which is both returned and written in memory
    """

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
                    v = {}

            else:
                articleTitle = article

            try:
                v = get_revisions_by_month(
                    articleTitle=articleTitle, timeStop=timeEnd, lang=language, f=""
                )
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
    """Converts dictionary to a flat table, which is bouth outputed and written in memory"""

    output_list = []
    for a, v in input_dict.items():
        for l, w in v.items():

            if w == {}:  # no revisions
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
    """Ensures revisions data is clean, if yes, the whole table is returned"""

    # expected columns and datatypes
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
    """Checks if table2 is clean, if yes, it is appended to existing table in memory"""

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

    concat_table.drop_duplicates()

    duplicates = concat_table.duplicated(
        subset=["Article", "Language", "Month"], keep=False
    )

    if duplicates.any():
        print("Duplicates!")
    else:
        print("No duplicates found.")

    concat_table.to_csv(filepath, index=False)
    return


def ETL(articles: list, langs: list, timeEnd: datetime, f: str = ""):

    if f == "reverted":
        filepath = "data/all_data/cleanWiki.csv"
    elif f == "reverted":
        filepath = "data/all_data/cleanWiki_reverted.csv"

    raw_data = pull_data(articles, langs, timeEnd, f=f)
    flattened_data = flatten_dict(raw_data)

    try:
        final_data = clean_data(flattened_data)
        append_table(filepath, final_data)
        write_protection_status(articles, langs)
        print("All good!")
    except:
        print("Failed data load")

    return


def get_protection_status(articles: list, langs: list):

    total = len(articles) * len(langs)
    counter = 1
    output_list = []
    for a in articles:

        for l in langs:
            print("Progress: starting {c}/{t}".format(c=str(counter), t=str(total)))
            print(a)
            print(l)
            p = get_protection(a, l)
            output_list.extend(p)
            counter += 1

    return pd.DataFrame(output_list)


def write_protection_status(articles: list, langs: list):

    filepath = "data/all_data/protections.csv"
    table1 = get_protection_status(articles, langs)
    table2 = pd.read_csv(filepath)
    concat_table = pd.concat([table1, table2], axis=0)
    concat_table.drop_duplicates()

    concat_table.to_csv(filepath, index=False)
    print("success!")


if __name__ == "__main__":

    ETL_reverted(
        articles=[
            "Benito_Mussolini",
            "Fall_of_the_Fascist_regime_in_Italy",
            "Italian_Social_Republic",
            "Rodolfo_Graziani",
            "Ardeatine_massacre",
            "Sant%27Anna_di_Stazzema_massacre",
            "Istrian-Dalmatian_exodus",
            "Foibe_massacres",
        ],
        langs=["it", "en"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )
