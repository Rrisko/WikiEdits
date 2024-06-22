# %%
import pandas as pd

ip_df = pd.read_csv("../data/detailed_data/detailedEdits_2024-06-14-11-41.csv")

# %%
################################################################################
##################   DELTA ANALYSIS   ########################################## 
################################################################################
from tqdm import tqdm
from protection_analysis import get_protections_for_article

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Turn off pandas SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)

def get_edit_delta_df(df: pd.DataFrame, article: str, lang: str):
    """filters for an article and language, sorts by timestamp and adds
    edit_delta, sign_delta, sign_change
    """
    # filter article and language
    df = df[(df["article"]==article) & (df["language"]==lang)]
    # sort by timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    # calc edit delta
    df['edit_delta'] = df['size'].diff()
    df.dropna(subset=['edit_delta'], inplace=True) # should only be the first
    # sign 0.5 for positive, -0.5 for negative - sign change is 1
    df['sign_delta'] = np.sign(df['edit_delta']) / 2
    df['sign_change'] = df['sign_delta'].diff().fillna(0).abs()
    df['month_year'] = df['timestamp'].dt.to_period('M')
    return df


def get_monthly_jerk(df: pd.DataFrame):
    """assumes the df already has
    * edit_delta
    * is filtered for one article and language
    """
    # Group by month and year, calculate the required metrics
    monthly_stats = df.groupby('month_year').agg(
        num_edits=('edit_delta', 'size'),
        num_sign_changes=('sign_change', 'sum')
    ).reset_index()
    # Calculate the number of sign changes relative to the total number of edits
    monthly_stats['relative_sign_changes'] = monthly_stats['num_sign_changes'] / monthly_stats['num_edits']
    # drop months with no edits
    monthly_stats = monthly_stats[monthly_stats['num_edits'] > 0]
    # Convert month_year to datetime for plotting
    monthly_stats['month_year'] = monthly_stats['month_year'].dt.to_timestamp()
    monthly_stats['rel_changes_squared'] = monthly_stats['relative_sign_changes'] ** 2
    return monthly_stats


def get_overall_jerk(df: pd.DataFrame, start="", end=""):
    """returns overall jerk for arbitrary specified time period
    * edit_delta
    * is filtered for one article and language
    """
    # filter if time period specified
    if start: df = df[(df['timestamp'] > start)]
    if end: df = df[(df['timestamp'] < end)]
    return df['sign_change'].sum() / len(df)


def plot_monthly_jerk(df: pd.DataFrame, start_time="", use_squared=False):
    """takes in the monthly stats calculated by calculate_monthly_jerk and plots
    the relative sign changes over time
    """
    if start_time:
        df = df[df['month_year'] > start_time]
    # Plotting the relative sign changes over time
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        x='month_year',
        y='rel_changes_squared' if use_squared else 'relative_sign_changes',
        data=df,
        # size='num_sign_changes'
        size='num_edits'
        )
    plt.xlabel('Month-Year')
    plt.ylabel('Relative Sign Changes' + (' Squared' if use_squared else ''))
    plt.title('Relative Sign Changes Over Time')
    plt.xticks(rotation=90)
    plt.show()


def plot_edit_delta(df: pd.DataFrame):
    """plot edit delta over time and the distribution of edit deltas
    """
    fig, axs = plt.subplots(2, 1, figsize=(10, 8))
    df.plot(x='timestamp', y='edit_delta', title='Edit delta over time', ax=axs[0])
    lb = int(df['edit_delta'].describe()['25%'])
    ub = int(df['edit_delta'].describe()['75%'])
    df[(df['edit_delta'] > lb) & (df['edit_delta'] < ub)
       ]['edit_delta'].plot(kind="hist", bins=100, title="Edit delta distribution", ax=axs[1])
    plt.tight_layout()
    plt.show()


def analyze_article_deltas(df: pd.DataFrame, article: str, lang: str):
    """Overall analysis of an article's edit deltas
    """
    filtered_df = get_edit_delta_df(df, article, lang)
    plot_edit_delta(filtered_df)
    monthly_stats = get_monthly_jerk(filtered_df)
    plot_monthly_jerk(monthly_stats)


def get_article_edit_metadata(df: pd.DataFrame):
    """Returns the earliest edit and num of overall edits for an article df
    * Must be a df filtered for article and language
    """
    earliest_edit = df['timestamp'].min()
    num_edits = len(df)
    return {'earliest_edit': earliest_edit, 'num_edits': num_edits}


def get_all_articles_jerk(article_list, df):
    """Overall analysis of an article's edit deltas
    """
    results = []
    for article in tqdm(article_list):
        df_article = get_edit_delta_df(df, article, "en")
        metadata = get_article_edit_metadata(df_article)
        oj = get_overall_jerk(df_article)
        results.append({'article': article, 'overall_jerk': oj} | metadata)
    
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('overall_jerk', ascending=False)
    return results_df


# %%

israel_palestine_articles = [
        "Nakba","Mandatory_Palestine","1948_Arab-Israeli_War","David_Ben-Gurion","Yasser_Arafat","Six-Day_War","Yom_Kippur_War","Hummus","Falafel","Shawarma","First_Intifada",
        "United_Nations_Partition_Plan_for_Palestine", "Intercommunal_conflict_in_Mandatory_Palestine", "Lehi_(militant_group)", "Irgun", "Ze'ev_Jabotinsky",
        "Haganah", "1947–1948_civil_war_in_Mandatory_Palestine", "1948_Arab–Israeli_War", "Yitzhak_Rabin", "Palmach", "Moshe_Dayan", "Jewish_exodus_from_the_Muslim_world", 
        "1936–1939_Arab_revolt_in_Palestine", "Amin_al-Husseini", "1948_Palestinian_expulsion_and_flight", "List_of_towns_and_villages_depopulated_during_the_1947–1949_Palestine_war", "Plan_Dalet", 
        "Abd_al-Qadir_al-Husayni", "1929_Hebron_massacre", "Causes_of_the_1948_Palestinian_expulsion_and_flight", "Deir_Yassin_massacre", "Menachem_Begin", "Kfar_Etzion_massacre", "Hebrew_language", 
        "Suez_Crisis", "Six-Day_War", "Egypt–Israel_peace_treaty", "Palestinian_Arabic", "Culture_of_Palestine", "Palestinian_cuisine", "Samih_al-Qasim", "Mahmoud_Darwish", "Origin_of_the_Palestinians"
]

""" Example usage of the functions
df48 = get_edit_delta_df(ip_df, "1948_Palestinian_expulsion_and_flight", "en")
plot_edit_delta(df48)
monthly_stats = calculate_monthly_jerk(df48)
plot_monthly_jerk(monthly_stats, "2015-01-01", use_squared=True)
"""

# analyze_article_deltas(ip_df, "Nakba", "en")
analyze_article_deltas(ip_df, "1948_Palestinian_expulsion_and_flight", "en")
get_protections_for_article('1948_Palestinian_expulsion_and_flight', 'en')

# %%

jerk_df = get_all_articles_jerk(israel_palestine_articles, ip_df)
jerk_df

# %%
################################################################################
##################   USER DATA METRICS   #######################################
################################################################################
def add_user_data_metrics_to_df(df: pd.DataFrame):
    """Can work with any df with user column
    """
    df["is_anon_uname"] = ip_df['user'].str.replace('.', '').str.isnumeric()
    df["frac_edits_on_this_article"] = df["article_edits"] / df["total_edits"]
    return df

def get_unique_users_for_article(df: pd.DataFrame) -> pd.DataFrame:
    """Expects a df filtered for one article and language.
    Returns a DataFrame with unique users.
    """
    return df.drop_duplicates(subset=['user'])

df48 = get_edit_delta_df(ip_df, "1948_Palestinian_expulsion_and_flight", "en")
df48 = add_user_data_metrics_to_df(df48)

udf48 = get_unique_users_for_article(df48)
# udf48["frac_edits_on_this_article"].plot(kind="hist", bins=100)
# print(udf48["frac_edits_on_this_article"].value_counts())
# print(len(df48[df48["total_edits"]==0]))
# print(len(df48))
# df48[df48["total_edits"]!=0]["frac_edits_on_this_article"].plot(kind="hist", bins=100)
# df48[df48["total_edits"]!=0]["frac_edits_on_this_article"].value_counts()
# df48[df48["frac_edits_on_this_article"]<=0.2]["frac_edits_on_this_article"].plot(kind="hist", bins=100)
df48[df48["frac_edits_on_this_article"]<=0.001]["frac_edits_on_this_article"].plot(kind="hist", bins=100)