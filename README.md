# WikiEdits

Project for course Data Science and Artificial Intelligence II

## Data

In folder `data`, subfolder `all_data` includes following files:

- `cleanWiki.csv` : summary table containing number of all article revisions per article, language, month. Columns are Article, Language, Month, Count
- `cleanWiki_reverted.csv` : summary table containing number of all article revisions that reverted a previous revision. Columns are Article, Language, Month, Count
- `protections.csv` : contains all intervals of time when the article was protected. Columns are article,language,start,end

## Code

Following files are located in folder `code`:

- `api_calls.py` contains functions that use WikiMedia API
- `ETL.py` uses function from `api_calls.py` to pull, transform, clean and write data into `data` folder
- `visualisatons.ipynb` contain charts with development of edits in time

## Examples

**Stepan Bandera** was leader of Ukrainian nationalists during 1930s and 1940s. His legacy is controversial, while some highlight the struggles against foreign powers on Ukrainian territory - Soviet Union, Poland and Germany, others see him as a war criminal due to ethnic cleansing targeting mostly ethnic Jews and Poles or collaboration with Nazi Germany. The spike of edits right after Russian full-scale invasion of Ukraine in 2022 is clearly noticeable

![image](https://github.com/Rrisko/WikiEdits/assets/115427248/0a907658-84b8-4864-b48e-8806a4d1a5e5)

**Foibe massacres** refer to mass killings and ethnic cleansing in the current territory along the borders of Italy, Slovenia and Croatia. Perpetrators were mostly aligned with Yugoslav Partisan movement and victims were mostly ethnic Italians. The topic is controversial due to questions of responsibility, collective guilt, reparations, etc. The Italian government designated February 10th as the day of rememberance and it is also around this date when edits (blue) and reverts (green) spike.

![image](https://github.com/Rrisko/WikiEdits/assets/115427248/01bd266d-4a0d-4382-8e9d-6afb07afd48a)
