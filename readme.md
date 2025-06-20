# Usage:
* navigate to directory with `readme.md` and `requirements.txt` file
* run command; `conda create -n <name of env e.g. chronic-disease-analyses> python=3.11.8`. Note that 3.11.8 must be the python version otherwise packages to be installed would not be compatible with a different python version
* once environment is created activate it by running command `conda activate`
* then run `conda activate chronic-disease-analyses`
* check if pip is installed by running `conda list -e` and checking list
* if it is there then move to step 8, if not then install `pip` by typing `conda install pip`
* if `pip` exists or install is done run `pip install -r requirements.txt` in the directory you are currently in
* run `python ./crawlers/extract_cdi.py -L https://www.kaggle.com/api/v1/datasets/download/payamamanat/us-chronic-disease-indicators-cdi-2023` to download chronic disease indicators data and transfer to s3
* run `python ./crawlers/extract_us_population_per_state_by_sex_age_race_ho.py` to extract raw population data per state per year. Note this uses selenium rather than beautifulsoup to bypass security of census.gov as downloading files using requests rather than clicking renders the downloaded `.csv` file as inaccessible