# Fetch Rewards Take Home Assessment

This project is created for Fetch rewards Take Home Assessment, where we analyze the data, try to come up with an Entity Relationship Model for the data to store in a data warehouse effectively and to find data quality issues

## How to use this repo
Step 1: Download the repo

`git clone https://github.com/ShravyaChalla/fetch_rewards_assessment.git`

`cd fetch_rewards_assessment`

Step 2: First you can open the fetch_relation_diagram.png to view the Entity Relation Diagram that would suit the data given

Step 3: Install the required packages using requirements.txt
`pip install -r requirements.txt`

Step 4: Run fetch_rewards.py from your terminal with the following command
`python fetch_rewards.py`

This script answers the questions provided in the the assessment link like 

* When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
* When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

Step 5: Go through the data_quality_exploration.ipynb notebook for detailed Data Quality assessment

Step 6: Read the message to the stakeholders in Email.txt


## Potential enhancements
In the interest of time this project is extremely rudimentary - a very simple and straightforward script that only focuses on answering the given questions. Depending on the end goal or use case there are many other possibilities to enhance this repository.
A few examples are listed below

* Using Object oriented programming we can make the scripts modular increasing readability and reusability of code. Normally I would structure my code this way
    * data_load.py with DataLoader class
    * utils.py with simple utility functions like common_filter, string functions, sql or cloud client creation etc
    * data_quality.py with DataQualityChecker class
    * feature_pipeline.py with PipelineRunner class that has the methods for queries

* Setting up CI/CD for deployment of the transformed data into the established Data Warehouse
* Sphinx documentation that builds document pages from doc strings

