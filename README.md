# Multinational Retail Data Centralisation

## Table of contents

1. [The Scope](#the-scope)
2. [Overview](#overview)
3. [What it does](#what-it-does)
4. [What I used](#what-i-used)
5. [What I learned](#what-i-learned)
6. [Instructions](#instructions)

## The Scope

You work for a multinational company that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

You will then query the database to get up-to-date metrics for the business.

## Overview

### What it does?

This project is designed to pull data from multiple different data sources including AWS RDS database, S3 buckets, PDf files, CVS files and more.
The Data that is pulled is then put into a pandas dataframe and cleaned with a variety of methods.
The cleaned data is then pushed to a local postgres database.
Running the sql functions in star_schema_functions.sql creates a star schema for the database.
The data_queries.sql file has the relevant queries to the questions commented within

### What I used

In this section I will list the technologies used and the reasons for them

- Pandas - I used this as i believe it to be the industry standard for data manipulation in python. It is very easy to use and in combination with an ipynb (Python notebook run with jupyter) visualising the data for exploratory data analysis.
- Tabula - This was the first time using this however it provides a really easy way to read data from a PDF and then put it into a Pandas DataFrame
- Boto3 - Boto3 is a very good way to connect to AWS technologies in python. The boto3.client offers an easy way to extract (and send) data from S3 Buckets
- Requests - This is my go to when dealing with APIs in Python. It is very easy to use and offers great responses for error handling
- Path (pathlib) - This was my first project experimenting with creating files locally. When i started this project I was using the OS library however i refactored this to use Path as it was easier to use.
- SQLAlchemy + psycopg2 - I have used this combination to manage my SQL interactions as i believe the SQLAlchemy engine object to be really easy to use. psycop2 alone is great for low level database operations but the high level approach that SQLAlchemy takes is great when working with objects

### What I learned

During this project I covered a lot of topics I havent really used before. Having comparatively done a lot of pandas I was fairly familiar with the functions I was using. I learned a lot about exploratory data analysis. With methods like info() and value_counts() you can tell a lot about the data regardless of the size and figure out what you need to do to clean the data. On top of this I feel like i have learned a lot about using python in OOP and making functions reusable across multiple data sources.
On the SQL side of things I learned a lot including the use of window functions. I think the most important part of the SQl side I learned is how to break down a complicated question into subqueries and build an answer to the question given

## Instructions

1. Clone this repo
2. Setup your credentials files - You will need 2 YAML files with your postgres details. The first will be to connect to AWS RDS database and will be to pull the data from this data source. The second will be for your local system. They should use the following format:
   `RDS_HOST: ******
RDS_PASSWORD: ******
RDS_USER: ******
RDS_DATABASE: ******
RDS_PORT: ******`
3. In the main.py file:

- Replace the aws_creds with the file path to your AWS RDS credentials.
- Replace the local_creds with the file path to your local database credentials
- On line 36: insert your API credentials in the header_dict and uncomment the line

4. Run the main.py file

If all your credentials are correct then the file will pull and clean data from all the sources and then push it to your local Postgres instance

5. Setting up the Star Schema

Run the functions in the star_schema_functions.sql file to setup the schema and create correct table associations

6. Should you choose to want the answers to the questions provided to me, run the queries in the data_queries.sql file.
