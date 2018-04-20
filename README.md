# Insight Data Engineering Project

## Introduction
Using Yelp's dataset to create a dashboard for business owners to analyze the top positive or negative words.

## Dataset
In total, there are :

* 5,200,000 user reviews
* Information on 174,000 businesses
* The data spans 11 metropolitan areas

## Purpose
Provide insight for business owners

## Technology
* Python


## Proposed Architecture
`
------------------        --------------------        -------------        -----------------
|                |        |                  |        |           |        |               |
| Kafka          | -----> | Spark            | -----> | Cassandra | -----> | Flask         |
| Data Ingestion |        | Batch Processing |        | Database  |        | Web Framework |
------------------        --------------------        -------------        -----------------
`