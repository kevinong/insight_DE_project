# Insight Data Engineering Project
# ReviewerRank
### Find top reviewers for your product


## Purpose
For each product, find the suitable users to provide quality reviews.


## Introduction
When people shop online, they tend to rely on product reviews heavily for their purchase decisions. If a product has little reviews or low quality reviews, mostly likely they won't help the product's sales. So by using this application, business owners can identity the best reviewers for that product and send them free products in exchange for quality reviews, and hopefully generate more revenue in the long run.

## Description
Using Amazon's product review dataset, I various metrics about a reviewer's review tendencies.


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