## About this project:

In this project I wanted to create and analyse a data set, using Python to crawl 
the data and Scala / Apache Spark to explore and analyse it.
I crawled some information about posts from the website www.worldstarhiphop.com


### File Descriptions / Instructions:

#### crawl_data.py
Start this script and it will crawl information from the website www.worldstarhiphop.com.
It will save the data into a .csv file in the same directory and create a file with information
about the data set. It will also create a smaller sub data frame for testing purposes and create an 
info file for it.

#### general_helper_functions.py
Some helper functions I wrote to help me out in data science projects.
I just started this so by the time you see this file there will probably not be a lot in here.

### /ScalaProcessing
In this folder i created an sbt project to use Apache Spark with Scala to analyse the data I have gathered.
Running it will produce a text file with some metrics that are gathered from the data.
There is a text file "Data Reports For WSHH.txt" in the project where I saved what I have discovered.

### TODOS:
I want to develop the sbt project some more. I want to especially get some statistical data on the
data set.