Hello Business Leaders,

Hope this email finds you well. I'm sure you are all curious to know what we are doing with the recent data you shared. Here's a quick rundown on the project along with a few follow up questions.

To start with we ran a few data quality checks on the json files shared. We used Python and SQL queries to form relations in the data and to uncover some issues. A simple method is to look for duplicates, missing values and establish common sense thresholds to figure out the data quality. With that we observed that most columns have missing values. Clear relationship between certain categories of columns like dates, number of items purchased or points earned is not provided. This makes it difficult for us to provide quality analysis on things like most commonly purchased brands. 

Given that basic context we are curious to know a few more details before investing in this project
* What do you hope to achieve with this exercise?
    * Is it to setup a Data Warehouse that helps with transacting the data?
    * To perform analytics on the existing data? Gather key points on brands, spending trends or demographics?
* How is this data collected? I am assuming this is all coming from the app through user interaction. But before it reaching our hands what other processes would the data go through?
* If we were to setup a Data Warehouse, what are the basic criteria you are looking for?
    * Which cloud platform and RDS database are we using?
    * Are there any limitations to the amount of data stored or the number of transactions you can make with the data?
    * How many requests per day on average do you expect to this database? Do you envision any scenario where there will be a spike in the request count?

At a minimum we would need the following to setup a production infrastructure
* A Batch Ingestion ETL Pipeline to gather the data
* Data transformation layer to convert the unstructured data to structured
* A Data Warehouse storage in an RDS database (Eg: Amazon Redshift)
* Secure access process like a single Sign-on (Eg: Okta) to access the data
* Indexing and Caching for frequently used data
* Load balancer for scaling API requests
* Github actions for CI/CD to production deployment
* Collect performance metrics through Amazon Cloudwatch and setup monitoring and alerts for anamoly detection
* We can visualize the metrics in AWS Quicksight or any other analytics tool like Tableau or PowerBI

We will be using Personal Identifiable Information(PII) from users. Receipts scanned will have sensitive user information that should be stored securely and follow US Data governance policies. 

Thank you for your patience in reading this far. Looking forward to hearing your thoughts on how we should proceed and learning the use case that we are plan to serve.

Regards,
Shravya Challa.