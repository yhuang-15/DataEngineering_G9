# JM0140-M-6 Data Engineering - Assignment 2
> Design and Implementation of Data Architecture and Data Processing Pipelines

GitHub Project page of group 9 for assignment 2 of the course Data Engineering
<br />
<br />
Names: <br />
Boris Binnendijk <br />
Gergo Boscardi <br />
Kit Chan <br />
Yikang Huang <br />
Virgil Sowirono <br />

## Introduction
The goal of the assignment was to design and implement a data architecture and multiple data processing pipelines utilising Apache Spark and Google Cloud Platform (GCP) services. Two seperate data processing pipelines were made, a short overview of these pipelines is given below.

Batch processing layer: Find out the airport name and coordinates which has the most and second least percentage of flight delay.

Stream processing layer: In every 5 minutes, find out the category and the amount of fraud in female and male customers that have greatest amount of fraud in total.

## Developing

### Prerequisites
An account on the Google Cloud Platform is necessary as this the platform where we will deploy our application. <br />
https://cloud.google.com/free

An API client is also necessary for testing purposes, one example for a client is Insomnia. <br />
https://insomnia.rest/

A local device is recommended because the Google Cloud Platform is not free, so in order to be efficient and cost-saving. We recommend to use a local machine for the API client, inspecting or modifying the code.

### Setting up Dev

To get started you can clone our GitHub project to your own desired repository.

```shell
git clone https://github.com/yhuang-15/DataEngineering_G9.git
```

## Style guide

Our python code is written in PEP-8 style, which is the style guide for python code by Guido van Rossum.
<br />
https://www.python.org/dev/peps/pep-0008/

## Dataset & Database

Two seperate datasets were used, one for the batch processing and one for the streaming processing. Both were found on the website Kaggle, which contains various available datasets.

Airline Delays:
<br />
https://www.kaggle.com/threnjen/2019-airline-delays-and-cancellations

Credit Card Fraud:
<br />
https://www.kaggle.com/kartik2112/fraud-detection?select=fraudTrain.csv
