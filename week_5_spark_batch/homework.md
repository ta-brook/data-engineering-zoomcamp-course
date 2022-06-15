## Week 5 Homework

In this homework we'll put what we learned about Spark
in practice.

We'll use high volume for-hire vehicles (HVFHV) dataset for that.

## Question 1. Install Spark and PySpark

`v3.0.3`


## Question 2. HVFHW February 2021

What's the size of the folder with results (in MB)?

`total 511M`

## Question 3. Count records 

How many taxi trips were there on February 15?

`425928`

## Question 4. Longest trip for each day

Trip starting on which day was the longest? 

`2021-02-11 : 75540 minute : 1259.0 hour`

## Question 5. Most frequent `dispatching_base_num`

Now find the most frequently occurring `dispatching_base_num` 
in this dataset.

` B02510| 3233664`

How many stages this spark job has?

`3`

## Question 6. Most common locations pair

Find the most common pickup-dropoff pair. 

`East New York / East New York |   45041`

For example:

"Jamaica Bay / Clinton East"

Enter two zone names separated by a slash

If any of the zone names are unknown (missing), use "Unknown". For example, "Unknown / Clinton East". 


## Bonus question. Join type

(not graded) 

For finding the answer to Q6, you'll need to perform a join.

What type of join is it?

And how many stages your spark job has?
