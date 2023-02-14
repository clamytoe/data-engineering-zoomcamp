# Analytics Engineering

## Data Domain Developments

* Massively parallel processing (MPP) databases
* Data-pipelines-as-a-service
* SQL-first
* Version control systems
* Self service analytics
* Data Governance

### Roles In Data Team

* Data Engineer: Prepares and maintains the infrastructure the data team needs.
* Analytics Engineer: Introduces the good software engineering practices to the efforts of data analytics and data scientists.
* Data Analyst: Uses data to answer questions and solve problems.

### Tooling

* Data Loading
* Data Storing
  * Cloud data warehouses like Snowflake, BigQuery, Redshift
* Data modeling
  * Tools like dbt or Dataform
* Data presentation
  * BI tools like google data studio, Looker, Mode or Tableau

### Data Model Concepts

![etl-v-elt](images/etl-v-elt.png)

### Kimball's Dimensional Modeling

* Objective
  * Deliver data undestandable to the business users
  * Deliver fast query performance
* Approach
  * Prioritize user understandability and query performance over non redundant data (3NF)
* Other approaches
  * Bill Inmon
  * Data vault

### Elements of Dimensional Modeling

![star-schema](images/star-schema.png)

* Facts tables
  * Measurements, metrics or facts
  * Corresponds to a business *process*
  * **verbs**
* Dimensions tables
  * Corresponds to a business *entity*
  * Provides context to a business process
  * **nouns**

### Architecture of Dimensional Modeling

![kitchen-modeling](images/kitchen-modeling.png)

## What is dbt?

**dbt**: Data build tool

dbt is a transformation tool that allows anyone that knows SQL to deploy analytics code follwoing software engineering best practices like modularity, portability, CI/CD, and documentation.

![dbt](images/dbt.png)

### How does dbt work?

Each model is:

* A *.sql file
* Select statement, no DDL or DML
* A file that dbt will compile and run in our DWH

![dbt-workflow](images/dbt-workflow.png)

### How to use dbt?

| dbt Core | dbt Cloud |
|:---------|:----------|
| Open-source project that allows the data transformation | Saas application to develop and manage dbt projects |
| * Builds and runs a dbt project (.sql and .yml files) | * Web-based IDE to develop, run and test a dbt project |
| * Includes SQL compilation logic, macros and database adapters | * Jobs orchestration |
| * Includes a CLI interface to run dbt commands locally | * Logging and Alerting |
| * Open source and free to use | * Integrated documentation |
| | * Free for individuals (one developer seat) |

### How are we going to use dbt?

* BigQuery
  * Development using cloud IDE
  * No local installation of dbt core
* Postgres
  * Develoment using a local IDE of your choice
  * Local installation of dbt core connecting to the Postgres database
  * Running dbt models through the CLI

![dbt-use](images/dbt-use.png)

### Starting a dbt project

#### Create a new dbt project

dbt provides a starter project with all the basic folders and files.

There are essentially two ways to use it:

* **With the CLI**
  * After having installed dbt locally and setup the *profiles.yml*, run `dbt init` in the path we want to start the project to clone the starter project.
* **With dbt cloud**
  * After having setup the dbt cloud credentials (repo and dwh) we can start the project from the web-based IDE.

### Development of dbt models

![dbt-anatomy](images/dbt-anatomy.png)
