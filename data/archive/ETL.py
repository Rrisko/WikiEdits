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
                    articleTitle=articleTitle, timeStop=timeEnd, lang=language, f=f
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

    if f == "":
        filepath = "data/all_data/cleanWiki.csv"
    elif f == "reverted":
        filepath = "data/all_data/cleanWiki_reverted.csv"
    else:
        print("f must be either empty string or 'reverted'")
        return

    raw_data = pull_data(articles, langs, timeEnd, f=f)
    flattened_data = flatten_dict(raw_data)

    try:
        final_data = clean_data(flattened_data)
        append_table(filepath, final_data)
        # write_protection_status(articles, langs)
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


def ETL_both(articles: list, langs: list, timeEnd: datetime):

    ETL(articles, langs, timeEnd, f="")
    ETL(articles, langs, timeEnd, f="reverted")
    write_protection_status(articles, langs)


if __name__ == "__main__":

    write_protection_status(
        articles=[
            "Ulysses_S._Grant",
            "Sherman's_March_to_the_Sea",
            "William_Tecumseh_Sherman",
            "Union_Army",
            "Confederate_States_Army",
            "Robert_E._Lee",
            "Joseph_E._Johnston",
            "Alexander_H._Stephens",
            "James_Longstreet",
            "United_Daughters_of_the_Confederacy",
            "Army_of_Northern_Virginia",
            "Jefferson_Davis",
            "Origins_of_the_American_Civil_War",
            "Confederate_States_of_America",
            "Abraham_Lincoln",
            "Battle_of_Gettysburg",
            "Judah_P._Benjamin",
            "John_C._Breckinridge",
            "Joseph_Wheeler",
            "P._G._T._Beauregard",
            "Franklin_Buchanan",
            "Nathan_Bedford_Forrest",
            "Ku_Klux_Klan",
            "John_C._Frémont",
            "Joseph_Hooker",
            "George_Meade",
            "Wilmington_massacre",
            "Red_Shirts_(United_States)",
            "United_Confederate_Veterans",
            "Confederate_History_Month",
            "Robert_E._Lee_Day",
            "Stonewall_Jackson",
            "Jim_Crow_laws",
            "John_Brown_(abolitionist)",
            "William_Lloyd_Garrison",
            "Frederick_Douglass",
            "Thaddeus_Stevens",
            "Battle_of_the_Wilderness",
            "Battle_of_Antietam",
            "Reconstruction_era",
            "Emancipation_Proclamation",
            "Thirteenth_Amendment_to_the_United_States_Constitution",
            "Slavery_in_the_United_States",
            "States'_rights",
            "Historiographic_issues_about_the_American_Civil_War",
        ],
        langs=["en", "de"],
    )

    write_protection_status(
        articles=[
            "Kolach_(bread)",
            "Paska_(bread)",
            "Pampushka",
            "Syrniki",
            "Rusyns",
            "Vyshyvanka",
            "Ukrainian_Soviet_Socialist_Republic",
            "Pereiaslav_Agreement",
            "West_Ukrainian_People's_Republic",
            "Massacres_of_Poles_in_Volhynia_and_Eastern_Galicia",
            "Orange_Revolution",
            "Ukrainian_War_of_Independence",
            "Principality_of_Kiev",
            "Kyiv_Pechersk_Lavra",
            "Golden_Gate,_Kyiv",
            "Bakhchysarai_Palace",
            "Khreshchatyk",
            "Kamianets-Podilskyi_Castle",
            "Saint_Sophia_Cathedral,_Kyiv",
            "Kobzar",
            "Hryhorii_Skovoroda",
            "Lesya_Ukrainka",
            "Rus'_people",
            "Zaporozhian_Cossacks",
            "Khmelnytsky_Uprising",
            "Ukrainian_People's_Republic",
            "Mykhailo_Hrushevsky",
            "Nikolai_Gogol",
            "Taras_Shevchenko",
            "Ukrainian_literature",
            "Ivan_Franko",
            "Ukrainian_Insurgent_Army",
            "Organisation_of_Ukrainian_Nationalists",
            "Pierogi",
        ],
        langs=["en", "de", "uk", "ru"],
    )

    write_protection_status(
        articles=[
            "United_Nations_Partition_Plan_for_Palestine",
            "Intercommunal_conflict_in_Mandatory_Palestine",
            "Lehi_(militant_group)",
            "Irgun",
            "Ze'ev_Jabotinsky",
            "Haganah",
            "1947–1948_civil_war_in_Mandatory_Palestine",
            "1948_Arab–Israeli_War",
            "Yitzhak_Rabin",
            "Palmach",
            "Moshe_Dayan",
            "Jewish_exodus_from_the_Muslim_world",
            "1936–1939_Arab_revolt_in_Palestine",
            "Amin_al-Husseini",
            "1948_Palestinian_expulsion_and_flight",
            "List_of_towns_and_villages_depopulated_during_the_1947–1949_Palestine_war",
            "Plan_Dalet",
            "Abd_al-Qadir_al-Husayni",
            "1929_Hebron_massacre",
            "Causes_of_the_1948_Palestinian_expulsion_and_flight",
            "Deir_Yassin_massacre",
            "Menachem_Begin",
            "Kfar_Etzion_massacre",
            "Hebrew_language",
            "Suez_Crisis",
            "Six-Day_War",
            "Egypt–Israel_peace_treaty",
            "Palestinian_Arabic",
            "Culture_of_Palestine",
            "Palestinian_cuisine",
            "Samih_al-Qasim",
            "Mahmoud_Darwish",
            "Origin_of_the_Palestinians",
        ],
        langs=["en", "de", "ar"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )

if 2 == 5:

    ETL_both(
        articles=[
            "Kolach_(bread)",
            "Paska_(bread)",
            "Pampushka",
            "Syrniki",
            "Rusyns",
            "Vyshyvanka",
            "Ukrainian_Soviet_Socialist_Republic",
            "Pereiaslav_Agreement",
            "West_Ukrainian_People's_Republic",
            "Massacres_of_Poles_in_Volhynia_and_Eastern_Galicia",
            "Orange_Revolution",
            "Ukrainian_War_of_Independence",
            "Principality_of_Kiev",
            "Kyiv_Pechersk_Lavra",
            "Golden_Gate,_Kyiv",
            "Bakhchysarai_Palace",
            "Khreshchatyk",
            "Kamianets-Podilskyi_Castle",
            "Saint_Sophia_Cathedral,_Kyiv",
            "Kobzar",
            "Hryhorii_Skovoroda",
            "Lesya_Ukrainka",
            "Rus'_people",
            "Zaporozhian_Cossacks",
            "Khmelnytsky_Uprising",
        ],
        langs=["en", "de", "ru", "uk"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )

    ETL_both(
        articles=[
            "Ulysses_S._Grant",
            "Sherman's_March_to_the_Sea",
            "William_Tecumseh_Sherman",
            "Union_Army",
            "Confederate_States_Army",
            "Robert_E._Lee",
            "Joseph_E._Johnston",
            "Alexander_H._Stephens",
            "James_Longstreet",
            "United_Daughters_of_the_Confederacy",
            "Army_of_Northern_Virginia",
            "Jefferson_Davis",
            "Origins_of_the_American_Civil_War",
            "Confederate_States_of_America",
            "Abraham_Lincoln",
            "Battle_of_Gettysburg",
        ],
        langs=["en", "de"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )

    ETL_both(
        articles=[
            "Ukrainian_People's_Republic",
            "Mykhailo_Hrushevsky",
            "Nikolai_Gogol",
            "Taras_Shevchenko",
            "Ukrainian_literature",
            "Ivan_Franko",
            "Ukrainian_Insurgent_Army",
            "Organisation_of_Ukrainian_Nationalists",
            "Pierogi",
            "",
        ],
        langs=["en", "uk", "ru", "de"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )

    ETL(
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
        f="reverted",
    )

    ETL_both(
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
        langs=["en", "uk", "ru", "de"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )

    ETL_both(
        articles=[
            "Nakba",
            "Mandatory_Palestine",
            "1948_Arab-Israeli_War",
            "David_Ben-Gurion",
            "Yasser_Arafat",
            "Six-Day_War",
            "Yom_Kippur_War",
            "Hummus",
            "Falafel",
            "Shawarma",
            "First_Intifada",
        ],
        langs=["en", "ar", "de"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )

    ETL_both(
        articles=[
            "Flight_and_expulsion_of_Germans_(1944–1950)",
            "Expulsion_of_Germans_from_Czechoslovakia",
            "Ústí_massacre",
            "Sudetenland",
            "Brno_death_march",
            "Beneš_decrees",
            "Sudetendeutsche_Landsmannschaft",
            "German_Expellees",
            "Recovered_Territories",
            "Flight_and_expulsion_of_Germans_from_Poland_during_and_after_World_War_II",
            "Oder–Neisse_line",
        ],
        langs=["en", "de", "cz", "pl"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )
    ETL_both(
        articles=[
            "Slovak_National_Uprising",
            "Jozef_Tiso",
            "Slovak_Republic_(1939-1945)",
            "Slovak_People%27s_Party",
            "Ján_Vojtaššák",
            "Alexander_Mach",
            "Andrej_Hlinka",
        ],
        langs=["en", "sk"],
        timeEnd=datetime(2007, 1, 1, 0, 0, 0),
    )
