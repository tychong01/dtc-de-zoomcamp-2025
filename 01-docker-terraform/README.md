### Question 1: Understanding docker first run

```
> docker run -it python:3.12.8 bash
root@21c93b6d011a:/# pip -V
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

Answer: `24.3.1`

### Question 2: Understanding Docker networking and docker-compose
Answer: `db:5433` 

### Question 3: Trip Segmentation Count

```sql
-- 1. Up to 1 mile
SELECT COUNT(*) as total_trip
FROM green_taxi
WHERE trip_distance <= 1;
-- 104,838

-- 2. In between 1 (exclusive) and 3 miles (inclusive)
SELECT COUNT(*) as total_trip
FROM green_taxi
WHERE trip_distance > 1 AND trip_distance <=3;
-- 104,838

-- 3. In between 3 (exclusive) and 7 miles (inclusive)
SELECT COUNT(*) as total_trip
FROM green_taxi
WHERE trip_distance > 3 AND trip_distance <=7;
-- 109,645

-- 4. In between 7 (exclusive) and 10 miles (inclusive)
SELECT COUNT(*) as total_trip
FROM green_taxi
WHERE trip_distance > 7 AND trip_distance <=10;
-- 27,688

-- 5. Over 10 miles
SELECT COUNT(*) as total_trip
FROM green_taxi
WHERE trip_distance > 10;
-- 35,202
```

Answer: `104,838; 199,013; 109,645; 27,688; 35,202`

### Question 4. Longest trip for each day
```sql
select date(gt.lpep_pickup_datetime) as pickup_date,
max(gt.trip_distance) as daily_longest_trip
from green_taxi gt 
group by date(gt.lpep_pickup_datetime)
order by daily_longest_trip desc
limit 1;
```



### Question 5. Three biggest pickup zones
```sql
    SELECT tz."Zone" as pickup_location, ROUND(SUM(gt.total_amount)::numeric, 3) as all_trip_total
    FROM green_taxi gt
    JOIN taxi_zone tz ON gt.pulocationid = tz."LocationID"
    WHERE DATE(gt.lpep_pickup_datetime) = '2019-10-18'
    GROUP BY tz."Zone"
    HAVING SUM(gt.total_amount) > 13000
    ORDER BY all_trip_total DESC;
```
Answer: `East Harlem North, East Harlem South, Morningside Heights`

### Question 6. Largest tip
```sql
    SELECT puz."Zone" as pickup_zone, doz."Zone" as dropoff_zone, ROUND(MAX(gt.tip_amount)::numeric, 3) as largest_tip
    FROM green_taxi gt
    join taxi_zone puz on gt.pulocationid  = puz."LocationID"
    JOIN taxi_zone doz ON gt.dolocationid = doz."LocationID"
    WHERE gt.lpep_dropoff_datetime::date BETWEEN '2019-10-01' AND '2019-10-31'
    AND puz."Zone" = 'East Harlem North'
    GROUP by pickup_zone, dropoff_zone
    ORDER BY largest_tip DESC;
```
Answer: `JFK Airport`

### Question 7. Terraform Workflow
Answer: `terraform init, terraform apply -auto-approve, terraform destroy`