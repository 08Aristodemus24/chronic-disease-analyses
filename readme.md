# Usage:
* navigate to directory with `Dockerfile` and `docker-compose.yaml` file
* make sure that docker is installed within you system
* run `make up` in terminal in the directory where the aforementioned files exist
* once docker containers are running go to `http://localhost:8080`
* sign in with `airflow` and `airflow` as username as password respectively
* trigger the directed acyclic graph (DAG) to run ETL pipeline which will basically automate (orchestrate) running the ff. commands in the background using airflow operators:
- `python ./operators/extract_cdi.py -L https://www.kaggle.com/api/v1/datasets/download/payamamanat/us-chronic-disease-indicators-cdi-2023` to download chronic disease indicators data and transfer to s3
- `python ./operators/extract_us_population_per_state_by_sex_age_race_ho.py` to extract raw population data per state per year. Note this uses selenium rather than beautifulsoup to bypass security of census.gov as downloading files using requests rather than clicking renders the downloaded `.csv` file as inaccessible
- `spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.563,org.apache.httpcomponents:httpcore:4.4.16 transform_us_population_per_state_by_sex_age_race_ho.py --year-range-list 2000-2009 2010-2019 2020-2023`
- `spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.563,org.apache.httpcomponents:httpcore:4.4.16 transform_cdi.py`
- `python ./operators/load_primary_tables.py`
- `python ./operators/update_tables.py`
* when all tasks are successful and pipeline shows all green checks we can visit https://chronic-disease-analyses.vercel.app/ to see the updated dashboard 

# Architecture:
architecture of the pipeline is as follows:
![cdi-pipeline](./figures%20&%20images/final-cdi-pipeline.jpg)