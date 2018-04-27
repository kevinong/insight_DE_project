# Insight Data Engineering Project
# Recommend users to product

## Dataset

Amazon User Review Data:
* 142.8 million reviews
* 9 millions products
* 1.4 million answered questions


## Purpose
For each product, find the suitable users to provide quality reviews.


## Technology
* Python
* Amazon S3
* Spark
* Redshift
* Flask


## Primary Engineering Challenges
* Joining large datasets
* Design the suitable schema storing the data
* Finding user "clusters"


## Specification/Constraints


## Proposed Architecture
```
 +--------------+
 | Amazon S3    |
 | Data Storage |
 +--------------+
        |
        |
        v
 +------------------+        +-----------+        +---------------+
 | Spark            | -----> | Redshift  | -----> | Flask         |
 | Batch Processing |        | Database  |        | Web Framework |
 +------------------+        +-----------+        +---------------+
```