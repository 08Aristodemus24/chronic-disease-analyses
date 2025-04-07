# Usage:
* navigate to directory with `readme.md` and `requirements.txt` file
* run command; `conda create -n <name of env e.g. chronic-disease-analyses> python=3.12.3`. Note that 3.11.4 must be the python version otherwise packages to be installed would not be compatible with a different python version
* once environment is created activate it by running command `conda activate`
* then run `conda activate chronic-disease-analyses`
* check if pip is installed by running `conda list -e` and checking list
* if it is there then move to step 8, if not then install `pip` by typing `conda install pip`
* if `pip` exists or install is done run `pip install -r requirements.txt` in the directory you are currently in
* run `python extract_population_per_us_state.py` 
* run notebook `extract_population_with_age_per_us_state.ipynb` with kernel `chronic-disease-analyses` 
* 