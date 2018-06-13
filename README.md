# Insight Data Engineering Project
# ReviewerRank
### Find top reviewers for your product


## Purpose
For each product, find the suitable users to provide quality reviews.

## Presentation
The powerpoint slides can be found here: https://docs.google.com/presentation/d/1mEQutAjwESCwI15MrSs4ov9HF1V67-JNE4JaFXCZKKE/edit?usp=sharing

## Introduction
When people shop online, they tend to rely on product reviews heavily for their purchase decisions. If a product has little reviews or low quality reviews, mostly likely they won't help the product's sales. So by using this application, business owners can identity the best reviewers for that product and send them free products in exchange for quality reviews, and hopefully generate more revenue in the long run.

## Description
Using Amazon's product review dataset, I extracted various metrics about Amazon users' review tendencies. And for each product, the top 100 most relavent users are found based on the users' review patterns.


## Technology
* Python
* Amazon S3
* Spark
* Postgres
* Dash by Plotly


## Architecture
```
 +--------------+
 | Amazon S3    |
 | Data Storage |
 +--------------+
        |
        |
        v
 +------------------+        +-----------+        +---------------+
 | Spark            | -----> | Postgres  | -----> | Dash          |
 | Batch Processing |        | Database  |        | Web Framework |
 +------------------+        +-----------+        +---------------+
```