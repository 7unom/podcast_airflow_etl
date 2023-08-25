# Project Overview

In this project we'll create a data pipeline using Airflow.
The pipeline will download podcast episodes and automatically store our results in a SQLite database that we can easily query.
The use of Airflow to do this project will help with a few things:
* We can schedule the project to run daily.
* Each task can run independently, and we get error logs.
* We can easily parallelize tasks and run in the cloud if we want to.
* We can extend this project more easily (add speech recognition, summaries, etc) using Airflow.

**Project Steps**

* Download the podcast metadata xml and parse
* Create a `SQLite` database to hold podcast metadata
* Download the podcast audio files using requests

# Local Setup

## Installation

To carry out this project, the following were downloaded and used locally:

* Apache Airflow 2.7.0
* Python 3.11.0
* Python packages
    * pandas
    * sqlite3
    * xmltodict
    * requests

## Data

If you want to view the podcast metadata page, it is [here](https://www.marketplace.org/feed/podcast/marketplace/). 

## Create database
The following below gives a description on how to create a `sqlite3 database` using the command line interface.
* Create database
    * Run `sqlite3 episodes.db`
    * Type `.databases` in prompt to create the db
* Run `airflow connections add 'podcasts' --conn-type 'sqlite' --conn-host 'episodes.db'`
*  `airflow connections get podcasts` to view connection info