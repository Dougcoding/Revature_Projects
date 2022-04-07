# Big Data - Project 1

## Description
Enabling Hive features with querying and optimizations where the main focal point is to understand the organization and positioning of data. Followed by querying the data, loading it into HDFS , and querying it into Hive. All of these processes are accomplished on Ubuntu.

## Technilogies Used
- Scala 2.13.8
- Spark 3.2.1
- Hadoop
- Hive

## Features
- Interactive CLI menu
- User login requirement
- User Permission levels
- Create users, Read user list, Update passwords, Delete user accounts
- Create, Read, and Delete tables
- Run 6 built-in queries on a Wine Quality Dataset https://www.kaggle.com/datasets/yasserh/wine-quality-dataset
- Implements partitioning and bucketing

To-do list:
- Encrypt passwords
- Allow custom queries
- Export queries into JSON files
- Import custom files

## Getting Started
- Instructions on setting up Scala and Spark using Intellij
- Download and install intelliJ https://www.jetbrains.com/idea/download/?fromIDE=#section=windows
- Download and install Spark 3.2.1 https://spark.apache.org/downloads.html

## Usage
Run the code in IntelliJ. Sign in with Admin account
- Username: douglas
- Password: dougpass

or

Basic account
- Username: bob
- Password: bobpass

The Admin account has full access to CRUD operations regarding user accounts. The Basic account can only read a list of existing user accounts. Both can run queries under the analytics option.
