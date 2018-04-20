# Insight Data Engineering Project

## Introduction
Using Yelp's dataset to create a dashboard for business owners to analyze the top positive or negative words.

## Dataset
In total, there are :

* 5,200,000 user reviews
* Information on 174,000 businesses
* The data spans 11 metropolitan areas

## Purpose
Provide insight for business owners so that they can know what are the most positive or negative aspects about their business.

## Technology
* Python
* Amazon S3
* Kafka
* Spark
* Redshift
* Flask


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