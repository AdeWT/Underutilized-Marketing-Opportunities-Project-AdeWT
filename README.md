# Underutilized Marketing Opportunities Exploration Project
A data pipeline, validation, NoSQL, and data visualization project by Ade William Tabrani.
---

## Preface
---
The author is a data analyst in a superstore holding company, asked to find underutilized marketing opportunities in available data. The C-level executives and marketing department are eager to increase revenue and profit by utilizing existing avenues further.

The dataset is obtained from [kaggle](https://www.kaggle.com/datasets/itssuru/super-store). 

---

## Workflow

### 1. Data Pipeline
In this scenario, the companies' sales and profit records are kept in PostgreSQL, hence the data analyst have to fetch the data from PostgreSQL. There are several expectations for the data to meet so Great Expectations python library was utilized before progressing further. The data analyst then writes a DAG code to automatically extract, transform (specifically, data cleaning such as transforming column names to lowercase separated by underscore, clearing out duplicates, and rows with missing data), and load (ETL) the data into ElasticSearch NoSQL database to utilize Kibana data visualization. 

DAG code, raw data, and clean data is available in the repository.

### 2. Data Visualization
Data visualization is done in Kibana with focus on which aspects are highly likely to be successful if utilized further. The area of focus is of area (state-wise and city-wise), customer segment, and product categories-sub-categories.

### 3. Data Storytelling
Analysis and report are then written for delivering insights to the stakeholders.

Visualization and insights is available in the repository.
