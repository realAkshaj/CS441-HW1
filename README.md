# Implementing MapReduce on Hadoop and EMR

### Author: Akshaj Kurra Satishkumar
### Email: akurr@uic.edu
### UIN: 659159323

## Introduction

This project focuses on creating a MapReduce program in Hadoop and deploying it to AWS Elastic MapReduce (EMR). The objective is to split the initial tasks of building a Large Language Model(LLM) to separate Mapper Reducers. I have implemented Mapper Reducers for Tokenization, Embedding, and CosineSimilarity Calculations. At the end of all this will be a CSV file which will contain the data/output of all the Map Reduce Tasks.

Video Link: https://youtu.be/X4wXVrma8ko
The video explains the deployment of Hadoop application in the AWS EMR Cluster and the project structure

### Environment
```
OS: Windows 11

IDE: IntelliJ IDEA 2022.2.3 (Ultimate Edition)

SCALA Version: 3.3.4

SBT Version: 1.10.1

Hadoop Version: 3.3.3

Java Version: 1.8.0
```


### Running the project

1) Clone this repository

```
https://github.com/realAkshaj/CS441-HW1.git
```
2) Open the project in IntelliJ




3) Use Classpath File
   
This is the recommended solution and can be done directly within IntelliJ. Instead of passing a long classpath via the command line, IntelliJ can generate a classpath file.

Steps to Use a Classpath File in IntelliJ:
Open IntelliJ and load your project.

Go to Run > Edit Configurations....

In the Run/Debug Configurations window, select your MainApp run configuration.

Scroll down to the Configuration tab, and find the option Shorten command line.

From the dropdown, choose JAR manifest or classpath file:

Classpath file is the most common and typically recommended option. It allows IntelliJ to store the classpath in a temporary file and reference it rather than passing it directly to the command line.
JAR manifest works by embedding the classpath inside the JAR manifest (this might require additional configuration if you're building a JAR).
Click Apply and then OK.

Re-run your application.
   

4) Run MainApp.


Or you can download the JAR and run it on Hadoop locally. Link - https://drive.google.com/file/d/1wC2_tjvyTHfFMy-4K1eRhPRt9fSA9-hm/view?usp=sharing

DO NOTE THAT THE JAVA VERSION IN IntelliJ is 1.8.0
testing_text.txt needs to be put in a dir input for this to run, 

code to put - 


hdfs dfs -mkdir input
hdfs dfs -put C:/link/to/text /input/

Code to run JAR - hadoop jar C:/Users/aksha/OneDrive/Desktop/functioningjars/HW1/akuMapRed-assembly-0.1.0-SNAPSHOT.jar MainApp hdfs:///input/testing.txt hdfs:///output



```

## Project Structure

The project comprises the following key components:


- **Shard Generation**: Shards are created by traversing the graph using a BFS algorithm. This method groups adjacent nodes together in the induced subgraphs, preventing edge loss. (before this happens, the code is cleaned)

- **Tokenization**: Converting the given text into tokens using the JTokkit Library.

- **MapReduce Jobs**: The project involves three MapReduce jobs:  Tokenization MR, Embedding MR, CosineSimilarity MR


## Prerequisites

Before starting the project, ensure that you have the necessary tools and accounts set up:

1. **Hadoop**: Set up Hadoop on your local machine or cluster.

2. **AWS Account**: Create an AWS account and familiarize yourself with AWS EMR.

3. **Java and Hadoop**: Make sure Java and Hadoop are installed and configured correctly.

5. **Git and GitHub**: Use Git for version control and host your project repository on GitHub.

6. **IDE**: Use an Integrated Development Environment (IDE) for coding and development.


## Conclusion

The project shows the importance of using Map Reduce to handle large datasets (Big Data) efficiently

For detailed instructions on how to set up and run the project, please refer to the project's documentation and README files.

**Note:** This README provides an overview of the project. For detailed documentation and instructions, refer to the project's YouTube video link and src files
