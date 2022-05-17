## Week 1 Homework

## Question 3. Count records 

How many taxi trips were there on January 15?

Consider only trips that started on January 15.

```
SELECT 
    SUM(total_amount)
FROM 
    yellow_taxi_trips
WHERE
    CAST(tpep_pickup_datetime AS date) = date '2021-01-15'
```

## Question 4. Largest tip for each day

Find the largest tip for each day. 
On which day it was the largest tip in January?

Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")

```
SELECT 
    CAST(tpep_pickup_datetime AS date) as date,
    MAX(tip_amount) as tip
FROM 
    yellow_taxi_trips
GROUP BY
    date
ORDER BY
    tip desc
LIMIT 5;
```

## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown" 

```
SELECT 
    COALESCE(zn."Zone", 'Unknown') AS destination,
    COUNT(1)
FROM 
    yellow_taxi_trips as t
JOIN zones as zn
ON zn."LocationID" = t."DOLocationID"
WHERE
    CAST(tpep_pickup_datetime AS date) = date '2021-01-14'
    AND
    t."PULocationID" = 43
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;
```

## Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East". 

```
SELECT 
    concat(COALESCE(znpu."Zone", 'Unknown'), ' / ', COALESCE(zndo."Zone", 'Unknown')),
    AVG(t.total_amount) AS total_amount
FROM 
    yellow_taxi_trips as t
JOIN zones as znpu
    ON znpu."LocationID" = t."PULocationID"
JOIN zones as zndo
    ON zndo."LocationID" = t."DOLocationID"
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;
```