# JM0140-M-6 Data Engineering - Assignment 1
> Deploying a Distributed ML Application on Big Data Infrastructures using the Containerization Approach

GitHub Project page of group 9 for assignment 1 of the course Data Engineering
<br />
<br />
Names: <br />
Boris Binnendijk <br />
Gergo Boscardi <br />
Kit Chan <br />
Yikang Huang <br />
Virgil Sowirono <br />

## Introduction

As our course is about data engineering, which is an approach to design and develop systems or applications that for example ingest, store and process data. These kind of systems are typically complex distributed sytems or applications which is also what our assignment was about. The task was to turn a machine learning application into a distributed application which had at least five differennt components. Another requirement was that this application should be containerized and deployed on Google Cloud.
<br /> <br />
An application on classifying handwritten digits was chosen. This was written using Python as our main language with usage of Flask for API management. Our data comes from a Postgres database which was built on a database shown in class.

## Developing

### Built With
A list of libraries and packages are listed downhere, this is only for clarity as these libraries and packages are already incorporated in the containerization of our application.

flask==2.0.1 <br />
pandas==1.0.5 <br />
numpy==1.19.1 <br />
nltk==3.5 <br />
scipy==1.4.1 <br />
spacy==2.3.2 <br />
h5py==2.10.0 <br />
requests <br />
scikit-learn==0.23.1 <br />
google-cloud-storage <br />
sqlalchemy==1.3.18 <br />
sqlalchemy-utils <br />
psycopg2-binary <br />

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

For the dataset we decided to use a dummy dataset from the sci-kit-learn package which can be used to classify digits from 0 and 10. <br />
https://scikit-learn.org/stable/modules/generated/sklearn.datasets.load_digits.html#sklearn.datasets.load_digits

A postgress database is used which contained three tables which were for the raw data, the preproccesed data and for the training / testing data.

## Licensing

State what the license is and how to find the text version of the license.
