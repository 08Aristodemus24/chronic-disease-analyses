# Final Pipeline Architecture:
![final-cdi-pipeline]('./figures%20%26%20images/final-cdi-pipeline.jpg')

# Business Use Case:
The business use case here was to solve the following problems of:
1. allocating healthcare resources inefficiently and not being able to design effective interventions because of the lack of insight what populations to target and what key drivers/risk factors of chronic disease are the most and least common.
2. being able to inform policy makers, 
3. Understand the Scope and Trends: They lack a clear, up-to-date picture of who is affected, where chronic diseases are most prevalent, and how these diseases are changing over time.
4. Measure Impact and Accountability: It's hard to tell if current health policies or programs are actually working to reduce chronic disease prevalence or improve patient quality of life.

Potential solutions:
* identify which chronic diseases should be prioritized in a healthcare system based on their occurrence (most and least common) has significant business and societal impact. 
* identify which demographics are most affected in terms of state, stratification, etc.

Reasoning:
* Optimized Resource Allocation: Healthcare systems operate with finite budgets and personnel. By identifying which diseases are most prevalent, resources (funding, medical staff, equipment, medication) can be directed where they will have the greatest impact on the largest number of people. This prevents wasteful spending on less impactful interventions.
* Improved Public Health Outcomes: Prioritization allows for targeted interventions, screening programs, and preventative measures for the most common or high-impact diseases, 

Outcome:
* Reduced incidence and prevalence of these diseases.
* Lower morbidity (illness) and mortality rates.
* Improved quality of life for a larger segment of the population.
* Cost Efficiency and Savings: Chronic diseases are the leading drivers of healthcare costs globally. By effectively managing and preventing the most common ones 
* Reduced hospitalizations and emergency room visits.
* Lower long-term treatment costs for complications.
* Increased focus on cost-effective primary prevention rather than expensive late-stage treatment.
* Potential for significant savings for healthcare providers, insurers, and governments.
* Enhanced Patient Care and Access: Strategic prioritization can lead to:
* Development of specialized care pathways for prioritized diseases.
* Better training for healthcare professionals in managing these conditions.
* Improved access to diagnostic tools and treatments for the most affected populations.
* Policy and Program Development: Your findings can directly inform health policy decisions, leading to: Development of national or regional health programs.
* Legislation aimed at addressing risk factors for prevalent diseases.
* Targeted public awareness campaigns.
* Workforce Productivity and Economic Impact: A healthier population is a more productive workforce. By reducing the burden of chronic diseases, you can: Decrease absenteeism and "presenteeism" (being at work but unwell).
* Reduce premature retirement due to illness.
* Contribute to overall economic growth and stability.
* Reduced Health Disparities: Analysis of disease prevalence often uncovers disparities across different socioeconomic groups or geographic regions. Prioritization efforts can then be directed to reduce these inequalities.
* Maximizing Reach/Population Health Impact: Focusing on the most common diseases (e.g., diabetes, hypertension, heart disease) means that interventions will benefit the largest number of people. This aligns with a public health strategy of achieving broad improvements.
* Efficient Program Scalability: Programs designed for highly prevalent diseases can often be scaled more efficiently due to larger target populations and potentially more available data/resources.
* Reduced Overall Healthcare Burden: High-prevalence diseases often account for the largest proportion of healthcare expenditures and utilization. Addressing them can yield the greatest overall reduction in system strain.
* Early Intervention/Prevention: Identifying least occurring but potentially severe diseases might highlight emerging threats or areas where early, targeted prevention efforts could prevent them from becoming widespread. This is more about proactive risk mitigation.
* Targeted Research & Development: For very rare but debilitating diseases, prioritization might mean directing research funding to find cures or better treatments, improving the lives of specific, often underserved, patient populations.
* Avoiding Future Crises: Analyzing the "least occurring" diseases could reveal clusters or risk factors for conditions that could become more prevalent if not addressed, acting as an early warning system.
* Addressing Neglected Conditions: Sometimes, "least occurring" can mean "neglected." Prioritizing such diseases could lead to more equitable healthcare for all, including those with less common conditions who often struggle to find resources or expertise.

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


