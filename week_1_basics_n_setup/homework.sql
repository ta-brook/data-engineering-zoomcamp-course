SELECT 
    COUNT(total_amount)
FROM 
    yellow_taxi_trips
WHERE
    CAST(tpep_pickup_datetime AS date) = date '2021-01-15'


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