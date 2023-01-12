# SQL CommAND Examples

## Sample Inner JOIN

Example 1

```sql
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  total_amount,
  concat(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
  concat(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM
  yellow_taxi_data t,
  zones zpu,
  zones zdo
WHERE
  t."PULocationID" = zpu."LocationID" AND
  t."DOLocationID" = zdo."LocationID"
LIMIT 100;
```

Example 2

```sql
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  total_amount,
  concat(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
  concat(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM
  yellow_taxi_data t JOIN zones zpu
    on t."PULocationID" = zpu."LocationID"
  JOIN zones zdo
    on t."DOLocationID" = zdo."LocationID"
LIMIT 100;
```

## Look for missing values

Example 1

```sql
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  total_amount,
  "PULocationID",
  "DOLocationID"
FROM
  yellow_taxi_data t
WHERE
  "PULocationID" is NULL or
  "DOLocationID" is NULL;
```

Example 2

```sql
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  total_amount,
  "PULocationID",
  "DOLocationID"
FROM
  yellow_taxi_data t
WHERE
  "PULocationID" NOT IN (SELECT "LocationID" FROM zones);
```

## Delete records

```sql
delete FROM zones WHERE "LocationID" = 142;
```

## Sample LEFT JOIN

```sql
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  total_amount,
  concat(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
  concat(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM
  yellow_taxi_data t LEFT JOIN zones zpu
    on t."PULocationID" = zpu."LocationID"
  LEFT JOIN zones zdo
    on t."DOLocationID" = zdo."LocationID"
LIMIT 100;
```

## Notes

* **INNER JOIN**: Only show records that match on both tables
* **LEFT JOIN**: If the record exists on the LEFT table but not on the right, show empty right value
* **RIGHT JOIN**: If the record exists on the right but not on the LEFT, show empty LEFT value
* **OUTER JOIN**: Show NULL values for either side.

## Changing datetime to date

Example 1

```sql
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  DATE_TRUNC('DAY', tpep_dropoff_datetime)
  total_amount
FROM
  yellow_taxi_data t
LIMIT 100;
```

Example 2

```sql
SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  CAST(tpep_dropoff_datetime AS DATE),
  total_amount
FROM
  yellow_taxi_data t
LIMIT 100;
```

## Group By

```sql
SELECT
  CAST(tpep_dropoff_datetime AS DATE) AS "day",
  COUNT(1)
FROM
  yellow_taxi_data t
GROUP BY
  CAST(tpep_dropoff_datetime AS DATE);
```

## Order By

Example 1

```sql
SELECT
  CAST(tpep_dropoff_datetime AS DATE) AS "day",
  COUNT(1)
FROM
  yellow_taxi_data t
GROUP BY
  CAST(tpep_dropoff_datetime AS DATE)
ORDER BY "day" ASC;
```

Example 2

```sql
SELECT
  CAST(tpep_dropoff_datetime AS DATE) AS "day",
  COUNT(1)
FROM
  yellow_taxi_data t
GROUP BY
  CAST(tpep_dropoff_datetime AS DATE)
ORDER BY "day" DESC;
```

### Date with largest number of trips

```sql
SELECT
  CAST(tpep_dropoff_datetime AS DATE) AS "day",
  COUNT(1) as "count"
FROM
  yellow_taxi_data t
GROUP BY
  CAST(tpep_dropoff_datetime AS DATE)
ORDER BY "count" DESC;
```

### Return maximum amount paid and passenger count

```sql
SELECT
  CAST(tpep_dropoff_datetime AS DATE) AS "day",
  COUNT(1) as "count",
  MAX(total_amount) as "total",
  MAX(passenger_count) as "passengers"
FROM
  yellow_taxi_data t
GROUP BY
  CAST(tpep_dropoff_datetime AS DATE)
ORDER BY "count" DESC;
```

## Group By Multiple Fields

> The `1` and `2` here refer to the two first select items.

```sql
SELECT
  CAST(tpep_dropoff_datetime AS DATE) AS "day",
  "DOLocationID", 
  COUNT(1) as "count",
  MAX(total_amount) as "total",
  MAX(passenger_count) as "passengers"
FROM
  yellow_taxi_data t
GROUP BY
  1, 2
ORDER BY "day" ASC, "DOLocationID" ASC;
```
