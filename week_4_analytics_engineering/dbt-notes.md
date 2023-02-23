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

#### The FROM clause of a dbt model

##### Sources

* The data loaded to our dwh that we use as sources for our models
* Configuration defined in the yml files in the models folder
* Used with the source macro that will resolve the name to the right schema, plus build the dependencies automatically
* Source freshness can be defined and tested

```yaml
sources:
    - name: staging
      database: production
      schema: trip_data_all

      loaded_at_field: record_loaded_at
      tables:
        - name: green_tripdata
        - name: yellow_tripdata
          freshness:
            error_after: {count: 6, period: hour}
```

##### Seeds

* CSV files stored in our repository under the seed folder
* Benefits of version controlling
* Equivalent to a copy command
* Recommended for data that doesn't change frequently
* Runs with `dbt seed -s file_name`

```sql
select
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref('taxi_zone_lookup') }}
```

##### Ref

* Macro to reference the underlying tables and views that were building the data warehouse
* Run the same code in any environment, it will resolve the correct schema for you
* Dependencies are built automatically

*dbt model*:

```sql
with green_data as (
  select *,
      'Green' as service_type
  from {{ ref('stg_green_tripdata') }}
),
```

*compiled code*:

```sql
with green_data as (
  select *,
      'Green' as service_type
  from "production", "dbt_victoria_mola", "stg_green_tripdata"
),
```

##### Macros

* Use control structures (e.g. if statements and for loops) in SQL
* Use environment variables in your dbt project for production deployments
* Operate on the results of one query to generate another query
* Abstrct snippets of SQL into reusable macros -- these are analogous to functions in most programming languages

*Definition of the macro*:

```sql
{#
    This macro returns the description of the payment_type
#}

{% macro get_payment_type_description(payment_type) ~%}

    case {{ payment_type }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end

{%~ endmacro %}
```

 *Usage of the macro*:

 ```sql
 select
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    congestion_surcharge::double precision
 from {{ source('staging', 'green_tripdata_2021_01') }}
 where vendorid is not null
 ```

 *Compiled code of the macro*:

 ```sql
 create or alter view production.dbt_vicorotia_mola.stg_green_tripdata as
 select
     case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end as payment_type_description,
    congestion_surcharge::double precision
 from "production", "staging", "green_tripdata_2021_01"
 where vendorid is not null
 ```

##### Packages

* Like libraries in other programming languages
* Standalone dbt projects, with models and macros that tackle a specific problem area
* By adding a package to your project, the package's models and macros will become part of your own project
* Imported in the **packages.yml** file and imported by running `dbt deps`
* A list of useful packages can be found in dbt package hub

*Specifications of the packages to import in the project*:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.0
```

*Usage of a macro from a package*:

```sql
{{ config(materialized='view) }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime]) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid) as integer) as ratecodeid,
```

##### Variables

* Variables are useful for defining values that should be used across the project
* With a macro, dbt allows us to provide data to models for compilation
* To use a variable we use the {{ var('...) }} function
* Variables can be defined in two ways:
  * In the `dbt_project.yml` file
  * On the command line

*Variable whose value we can change via CLI*:

```sql
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=trie) %}

  limit 100

{% endif %}
```

*Global variable we define under project.yml*:

```yml
vars:
  payment_type_values: [1, 2, 3, 4, 5, 6]
```

##### How To Use Seeds

Seeds are used to add CSV files to our database. Just place the csv file in the seeds directory and run the following command:

```sql
dbt seed
```

Use the following command to refresh the table instead of adding to it:

```sql
dbt seed --full-refresh
```

#### To run all models

In order to run all models that are associated with fact_trips, we use the following command:

```sql
dbt build --select +fact_trips
```

## Testing and documenting dbt models

### Tests

* Assumptions that we make about our data
* Tests in dbt are essentially a `select` sql query
* These assumptions get compiled to sql that returns the amount of failing records
* Test are defined on a column in the .yml file
* dbt provides basic tests to check if the column values are:
  * Unique
  * Not null
  * Accepted values
  * A foreign key to another table
* You can create your custom tests as queries

### Documentation

* dbt provides a way to generate documentation for your dbt project and render it as a website.
* The documentation for your project includes:
  * Information about your project
    * Model code (both from .sql file and compiled)
    * Model dependencies
    * Sources
    * Auto generated DAG from the ref and source macros
    * Descriptions (from .yml file) and tests
  * Information about your data warehouse (information_schema):
    * Column names and data types
    * Table stats like size and rows
* dbt docs can also be hosted in dbt cloud

#### To run tests

```sql
dbt test
```

#### Build the project

Once all tests are passing the project can be built with the build command:

```sql
dbt build
```

### Deployment of a dbt project

#### What is deployment?

* Process of running the models we created in our development environment in a production environment
* Develoment and later deployment allows us to continue building models and testing them without affecting our production environment
* A deployment environment will normally have a different schema in our data warehouse and ideally a different user
* A development - deployment workflow will be something like:
  * Develop in a user branch
  * Open a PR to merge into the main branch
  * Run the new models in the production environment using the main branch
  * Schedule the models

#### Running a dbt project in production

* dbt cloud includes a scheduler where to create jobs to run in production
* A single job can run miltiple commands
* Jobs can be triggered manually or on schedule
* Each job will keep a log of the runs over time
* Each run will have the logs for each command
* A job could also generate documentation, that could be viewed under the run information
* If dbt source freshness was run, the results can also be viewed at the end of a job

#### What is Continuous INtegration (CI)

* CI is the practice of regularly merge development branches into a central repository, after which automated builds and tests are run.
* The goal is to reduce adding bugs to the production code and maintain a more stable project.
* dbt allows us to enable CI on pull requests.
* Enabled via webhooks from GitHub or GitLab.
* When a PR is ready to be merged, a webhooks is received in dbt Cloud that will enqueu a new run of the specified job.
* The run of the CI job will be against a temporary schema.
* No PR will be able to be merged unless the run has been completed successfully.

#### Production Environment

* On dbt, create a new environment `Production`
* Create a New Job
*

### Deployment of a dbt project locally

* Process of running the models we created in our development environment in a production environment
* Development and later deployment allows us to continue building models and testing them without affecting our production environment
* A deployment environment will normally have a different schema in our data warehouse and ideally a different user
* A development - deployment workflow will be something like:
  * Develop in a user branch
  * Open a PR to merge into the main branch
  * Merge the branch to the main branch
  * Run the new models in the production environment using the main branch
  * Schedule the models

#### Modify Profiles yaml file

You will to define the production environment:

```yaml
taxi_rides_ny_bq:
  target: dev
  outputs:
    dev:
      dataset: dtc_ny_taxi_tripdata
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: /home/clamytoe/.ssh/dtc-de-course-374214-6cf927694a1d.json
      location: US
      method: service-account
      priority: interactive
      project: dtc-de-course-374214
      threads: 4
      type: bigquery
    prod:
      dataset: ny_taxi
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: /home/clamytoe/.ssh/dtc-de-course-374214-6cf927694a1d.json
      location: US
      method: service-account
      priority: interactive
      project: dtc-de-course-374214
      threads: 4
      type: bigquery
```

Run with: `dbt build -t prod`

### Visualizing the data with Google Data Studio

* Navigate to [https://lookerstudio.google.com/](https://lookerstudio.google.com/)
* Create a Data Source
* Select BigQuery
* Select your Project
* Select your Dataset
* Select your Table
* Connect
* Change the Default Aggregation for categorical columns from Sum to None
* Create Report

### Use Metabase as an alternative

Launch the docker image: `docker run -d -p 3000:3000 --name metabase metabase/metabase`
