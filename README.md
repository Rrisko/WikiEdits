# WikiEdits

Project for course Data Science and Artificial Intelligence II


## How does Wiki work?

In general, registered and unregistered users are able to edit articles.

However, some articles are under [protection](https://en.wikipedia.org/wiki/Wikipedia:Protection_policy) which places a limitation on who is allowed to make changes.

## What we did

- module containing functions to pull and process whole edit history for given list of articles and language versions, as well as the history of protections placed on them

- implemented several metrics for analysing and comparing the edit activity and protection history, some of them new, some based on existing literature

- pulled a sample of 131 articles in different languages (371 unique article-language combinations)

- exploratory analysis on the sample

- calculation and analysis of the metrics for the sample

## Our case study

We selected articles related to three topics: **War in Ukraine**, **War in Gaza**, **US Civil War**, as there is a high chance of *edit wars* or increased attention due to real-world events. As the first two wars are still ongoing, we limited the selection to articles that do not cover current topics, and the edit activity is not expected to be correlated with real-world events for a legitimate reason. Articles related to those topics in our sample are rather related to culture or history.

## Data

In folder `data`, you can find following files:

- `detailed_data` : folder includes tables where each observation corresponds to a unique edit on a wiki article. An edit has following attributes: user, timestamp, size (of the whole article after edit), reverted (binary), reversion (binary), article (English), language (language code used by Wikipedia), total_edits (number of edits done by user on the all articles belonging to the wiki language version), article_edits (number of edits user has done on this article)
- `protections_data` : folder includes tables where each observation corresponds to a unique protection log of an article. A protection log has following attributes: language, title (in the language of the article), timestamp, user, action, comment, type, level, expiry, article (English)

## Code

Following files are located in folder `code`

## Some interesting examples

**Foibe massacres** refer to mass killings and ethnic cleansing in the current territory along the borders of Italy, Slovenia and Croatia. Perpetrators were mostly aligned with Yugoslav Partisan movement and victims were mostly ethnic Italians. The topic is controversial due to questions of responsibility, collective guilt, reparations, etc. The Italian government designated February 10th as the day of rememberance and it is also around this date when edits (blue) and reverts (green) spike.

![image](https://github.com/Rrisko/WikiEdits/assets/115427248/01bd266d-4a0d-4382-8e9d-6afb07afd48a)

**Stepan Bandera** was leader of Ukrainian nationalists during 1930s and 1940s. His legacy is controversial, while some highlight the struggles against foreign powers on Ukrainian territory - Soviet Union, Poland and Germany, others see him as a war criminal due to ethnic cleansing targeting mostly ethnic Jews and Poles or collaboration with Nazi Germany. The spike of edits right after Russian full-scale invasion of Ukraine in 2022 is clearly noticeable

![image](https://github.com/Rrisko/WikiEdits/assets/115427248/0a907658-84b8-4864-b48e-8806a4d1a5e5)

Not even **borscht** was able to escape the consequences of 2022 invasion. The arguments about the origins of the dish (Ukrainian/Russian/general Eastern European) resulted in large increase of edits and reverts.

![image](https://github.com/Rrisko/WikiEdits/assets/115427248/66a7d260-732c-48d4-bc09-5081f100851f)

