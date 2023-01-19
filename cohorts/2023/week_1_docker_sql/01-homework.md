# Week 1 Homework

In this homework we'll prepare the environment and practice with Docker and SQL

## Question 1. Knowing docker tags

Run the command to get information on Docker

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file*

```bash
docker build --help

Usage:  docker build [OPTIONS] PATH | URL | -

Build an image from a Dockerfile

Options:
      --add-host list           Add a custom host-to-IP mapping (host:ip)
      --build-arg list          Set build-time variables
      --cache-from strings      Images to consider as cache sources
      --disable-content-trust   Skip image verification (default true)
  -f, --file string             Name of the Dockerfile (Default is 'PATH/Dockerfile')
      --iidfile string          Write the image ID to the file
...
```

- `--imageid string`
- **`--iidfile string`**
- `--idimage string`
- `--idfile string`

## Question 2. Understanding docker first run

Run docker with the python:3.9 image in an iterative mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list).
How many python packages/modules are installed?

```bash
root@83c35a214ac0:/# pip list
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
```

- 1
- 6
- **3**
- 7

## Prepare Postgres

Run Postgres and load data as shown in the videos We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

## Question 3. Count records

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15.

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

```sql
SELECT 
  COUNT(*) as total_trips
FROM 
  green_taxi_data
WHERE 
  DATE(lpep_pickup_datetime) = '2019-01-15' AND 
  DATE(lpep_dropoff_datetime) = '2019-01-15';
```

*output*:

```bash
+-------------+
| total_trips |
|-------------|
| 20530       |
+-------------+
```

- 20689
- **20530**
- 17630
- 21090

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

```sql
SELECT 
    DATE(lpep_pickup_datetime)  as pickup,
    ROUND(SUM(trip_distance)) AS distance
FROM 
    green_taxi_data
GROUP BY pickup
ORDER BY distance DESC
LIMIT 1;
```

*output*:

```bash
+------------+----------+
| pickup     | distance |
|------------+----------|
| 2019-01-25 | 83746.0  |
+------------+----------+
```

```sql
WITH trips_by_day AS (
  SELECT 
    DATE(lpep_pickup_datetime) as day,
    ROUND(sum(trip_distance)) as total_distance
  FROM 
    green_taxi_data
  GROUP BY 
    day
)
SELECT 
  day, 
  total_distance
FROM 
  trips_by_day
ORDER BY 
  total_distance DESC
LIMIT 4;
```

*output*:

```bash
+------------+----------------+
| day        | total_distance |
|------------+----------------|
| 2019-01-25 | 83746.0        |
| 2019-01-17 | 80241.0        |
| 2019-01-11 | 79742.0        |
| 2019-01-10 | 79531.0        |
+------------+----------------+
```

```sql
SELECT DATE(lpep_pickup_datetime) AS pickup_date,
 ROUND(SUM(trip_distance)) AS distance
FROM green_taxi_data
WHERE DATE(lpep_pickup_datetime) = '2019-01-18'
 OR DATE(lpep_pickup_datetime) = '2019-01-28'
 OR DATE(lpep_pickup_datetime) = '2019-01-15'
 OR DATE(lpep_pickup_datetime) = '2019-01-10'
GROUP BY pickup_date
ORDER BY distance DESC;
```

*output*:

```bash
+------------+----------+
| pickup     | distance |
|------------+----------|
| 2019-01-10 | 79531.0  |
| 2019-01-18 | 76829.0  |
| 2019-01-15 | 74856.0  |
| 2019-01-28 | 74054.0  |
+------------+----------+
```

- 2019-01-18
- 2019-01-28
- 2019-01-15
- **2019-01-10**

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?

```sql
SELECT 
  COUNT(CASE WHEN passenger_count = 2 THEN 1 END) as trips_with_2_passengers,
  COUNT(CASE WHEN passenger_count = 3 THEN 1 END) as trips_with_3_passengers
FROM 
  green_taxi_data
WHERE 
  lpep_pickup_datetime >= '2019-01-01' AND 
  lpep_pickup_datetime < '2019-01-02';
```

*output*:

```bash
+-------------------------+-------------------------+
| trips_with_2_passengers | trips_with_3_passengers |
|-------------------------+-------------------------|
| 1282                    | 254                     |
+-------------------------+-------------------------+
```

- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- **2: 1282 ; 3: 254**
- 2: 1282 ; 3: 274

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

```sql
WITH astoria_trips AS
 (SELECT g."PULocationID",
   g."DOLocationID",
   g."tip_amount"
  FROM green_taxi_data g
  WHERE g."PULocationID" IN
    (SELECT z."LocationID"
     FROM zones z
     WHERE z."Zone" = 'Astoria' ) )
SELECT zones."Zone" AS dropoff_zone,
 MAX(astoria_trips."tip_amount") AS max_tip
FROM astoria_trips
JOIN zones ON astoria_trips."DOLocationID" = zones."LocationID"
GROUP BY dropoff_zone
ORDER BY max_tip DESC
LIMIT 1;
```

*output*:

```bash
+-------------------------------+---------+
| dropoff_zone                  | max_tip |
|-------------------------------+---------|
| Long Island City/Queens Plaza | 88.0    |
+-------------------------------+---------+
```

- Central Park
- Jamaica
- South Ozone Park
- **Long Island City/Queens Plaza**

## Submitting the solutions

- Form for submitting: TBA
- You can submit your homework multiple times. In this case, only the last submission will be used.

Deadline: 26 January (Thursday), 22:00 CET

## Solution

We will publish the solution here
