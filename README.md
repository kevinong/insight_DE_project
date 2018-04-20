# Insight Data Engineering Project

## Introduction
Create a dashboard for business owners to analyze the top positive or negative words in their reviews using sentiment analysis.

## Potential Datasets
Yelp:
* 5,200,000 user reviews
* Information on 174,000 businesses
* The data spans 11 metropolitan areas
* ~ 7 GB

Amazon:
* 142.8 million reviews
* ~ 20 GB


## Purpose
Provide insight for business owners so that they can know what are the most positive or negative aspects about their business.

## Technology
* Python
* Amazon S3
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