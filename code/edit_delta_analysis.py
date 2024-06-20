# %%
import pandas as pd

ip_df = pd.read_csv("../data/detailed_data/detailedEdits_2024-06-14-11-41.csv")

# %%
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def add_edit_delta(df: pd.DataFrame, article: str, lang: str):
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


def calculate_monthly_jerk(df: pd.DataFrame):
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
    filtered_df = add_edit_delta(df, article, lang)
    plot_edit_delta(filtered_df)
    monthly_stats = calculate_monthly_jerk(filtered_df)
    plot_monthly_jerk(monthly_stats)


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
df48 = add_edit_delta(ip_df, "1948_Palestinian_expulsion_and_flight", "en")
plot_edit_delta(df48)
monthly_stats = calculate_monthly_jerk(df48)
plot_monthly_jerk(monthly_stats, "2015-01-01", use_squared=True)
"""

# analyze_article_deltas(ip_df, "Nakba", "en")
analyze_article_deltas(ip_df, "1948_Palestinian_expulsion_and_flight", "en")
