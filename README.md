# Insight Data Engineering Project
# Recommend users to products

## Dataset

Amazon User Review Data:
* 142.8 million reviews
* 9 millions products
* 1.4 million answered questions


## Purpose
For each product, find the suitable users to provide quality reviews.


## Introduction
Find the users related to a certain product, then compute various metrics that indicates the user's "quality", such as average star rating, helpfulness votes, etc..


## Technology
* Python
* Amazon S3
* Spark
* Redshift
* Flask


## Primary Engineering Challenges
* Joining large datasets
* Design the suitable schema storing the data
* Finding product "clusters" and related users


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