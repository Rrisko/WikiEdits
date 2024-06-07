# %%
import pandas as pd

protections = pd.read_csv('../data/all_data/protections.csv')
wiki = pd.read_csv('../data/all_data/cleanWiki.csv')
revert_wiki = pd.read_csv('../data/all_data/cleanWiki_reverted.csv')

# %%
# convert start and end dates to datetime
protections['start'] = pd.to_datetime(protections['start'], format='mixed')
protections['end'] = pd.to_datetime(protections['end'], format='mixed')

# count nan values
print(protections['start'].isna().sum())
print(protections['end'].isna().sum())

# %% 
import requests

def fetch_reversions(page, lang='en', only_reverted=False, limit=None):
    url = f"https://api.wikimedia.org/core/v1/wikipedia/{lang}/page/{page}/history"
    if only_reverted:
        url += '?reverted=true'

    history = []
    while True:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            history.extend(data['revisions'])
            print("fetched revisions up to: ", data['revisions'][-1]['timestamp'])
            if limit and len(history) >= limit:
                break

            if 'older' in data:
                url = data['older']
            else:
                break
        else:
            print(f"Request failed with status code {response.status_code}")
            return

    df = pd.DataFrame(history)
    df = pd.concat([df.drop(['user'], axis=1), df['user'].apply(pd.Series)], axis=1)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

df = fetch_reversions("1948_Palestinian_expulsion_and_flight", limit=5000)
# %% 
# lineplot delta column over time
import seaborn as sns
import matplotlib.pyplot as plt

# %%
import numpy as np
df['sign_delta'] = np.sign(df['delta'])

# Step 2: Calculate the difference in consecutive signs
df['sign_change'] = df['sign_delta'].diff().fillna(0)

# Step 3: Sum up the absolute values of these differences over a rolling window (e.g., 3 periods)
window_size = 50
df['jerk'] = df['sign_change'].abs().rolling(window=window_size).sum()

# plot jerk column over time
plot = sns.lineplot(x='timestamp', y='jerk', data=df)

# show date on x-axis
plot.xaxis_date()
plt.xticks(rotation=90, ha='right')

# TODO: calculate sign changes window based on time period, not number of reversions
