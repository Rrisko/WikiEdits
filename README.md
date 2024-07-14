# WikiEdits

Project for course Data Science and Artificial Intelligence II. We pulled history of 131 articles, each in at least two language versions connected to controversial topics and performed various kinds of analyses on them.

## How does Wiki work?

In general, registered and unregistered users are able to edit articles. However, some articles are under [protection](https://en.wikipedia.org/wiki/Wikipedia:Protection_policy) which places a limitation on who is allowed to make changes.

You can see a history of an article by clicking on "View history", usually on top right corner. A history page contains logs of all previous versions, allowing you to compare changes and see the users who were modifying. For example, [this](https://en.wikipedia.org/w/index.php?title=Vienna&action=history) is the history page of English language article about Vienna. You can also scroll through protection history of an article by searching for it [here](https://en.wikipedia.org/wiki/Special:Log?type=protect).

Wikipedia also provides multiple API services. In this project we used endpoints by **MediaWiki** ([documentation](https://www.mediawiki.org/wiki/API:Main_page)) and **Wikimedia** ([documentation](https://api.wikimedia.org/wiki/Core_REST_API)).

## What we did

- module containing functions to pull and process whole edit history for given list of articles and language versions, as well as the history of protections placed on them

- implemented several metrics for analysing and comparing the edit activity and protection history, some of them new, some based on existing literature

- pulled a sample of 131 articles in different languages (371 unique article-language combinations)

- exploratory analysis on the sample

- calculation and analysis of the metrics for the sample

## Our case study

We selected articles related to three topics: **War in Ukraine**, **War in Gaza**, **US Civil War**, since there is a high chance of *edit wars* or increased attention due to real-world events. As the first two wars are still ongoing, we limited the selection to articles that do not cover current topics, and the edit activity is not expected to be correlated with real-world events for a legitimate reason. Articles related to those topics in our sample are rather related to culture or history.

You can read more in our [project report](https://github.com/Rrisko/WikiEdits/blob/main/final_report.pdf), where we explain the metrics in more detail and present results of our case study.

## Data

In folder `data`, you can find following files:

- `detailed_data` : folder includes tables where each observation corresponds to a unique edit on a wiki article. An edit has following attributes: user, timestamp, size (of the whole article after edit), reverted (binary), reversion (binary), article (English), language (language code used by Wikipedia), total_edits (number of edits done by user on the all articles belonging to the wiki language version), article_edits (number of edits user has done on this article)
- `protections_data` : folder includes tables where each observation corresponds to a unique protection log of an article. A protection log has following attributes: language, title (in the language of the article), timestamp, user, action, comment, type, level, expiry, article (English)
- `raw_data` : folder includes edit and protection data before transformation into a flat format

## Code

Following files are located in folder `code` :

- `api_functions.py` : various functions for pulling or transforming data are grouped here
- `ETL.py` : here, functions from `api_functions.py` are imported and `ETL()` function is built from them. Using `ETL()` function you can download additional data for both protection and edit history.
- `edit_delta_analysis.ipynb` : in this notebook, article revisions are analysed based on the total size of a revision and the direction of the size change (increase/decrease)
- `protection_analysis.ipynb` : in this notebook, we analyse protection logs, compare topics and language versions and look for trends in time
- `correlation_burstiness_analysis.ipynb` : in this notebook, we calculate and visualise correlations of edit activities between pairs of articles as well as burstiness metric

## How to run

There are several options for replicating or expanding our work. 

- using `ETL()` function from ETL.py file, you can download data for further articles on Wikipedia. The function takes following arguments: `articles` (list of article names in English), `langs` (list of short language codes) and `topic` (used for naming the extracted and transformed articles. For example: `ETL(articles = ["Vienna", "Prater", "Danube", "St._Stephen's_Cathedral,_Vienna", "Belvedere,_Vienna", "Schönbrunn_Palace", "Hofburg", "Wiener_schnitzel"], langs = ["en", "de", "sk", "he", "uk", "lv"], topic = "Vienna")`
- having downloaded new data, you can perform same analyses we did using the three notebooks in `code` folder. Necessary is to change names of the imported files in the beginning of each notebook.
- you can also run or implement new analyses using the data we have already downloaded and transformed (131/371 articles) using the notebooks. Data is stored in `data` folder
- important: due to specificities of some languages, `ETL()` might throw error or not work as expected. Currently supported languages are English(en), German(de), Arabic(ar), Ukrainian(uk), Russian(ru), Slovak(sk), Czech(cs), Polish(pl), Italian(it). To expand this list, changes in `api_functions.py` are required, especially the functions that transform timestamps from protection logs.
