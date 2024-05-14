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

