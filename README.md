# Analyze User on Stack OverFlow

## Description
- Create a data pipeline:
  
  <img src="DataPipeline.JPG" alt="Alt text" title="Optional title">

- The dataset consists of two files: Questions and Answers. Download from google drive 
  <p>+ Questions contains all the questions on Stack Overflow in a certain time period, including: ID, Title, BodyQuestion, OwnerUserId, CreationDate, ClosedDate, Score</p>
  <p>+ Answers contains all the answers to each question in Questions, and has the following fields: ID, BodyAnswer, QuestionID, OwnerUserId, CreationTime, Score</p>
- Config Hadoop, Spark, Connect pySpark to MongoDB. Read, write data with MongoDB in Spark
- Analize some topic:
    <p>+ Count the number of times programming languages appear.</p>
    <p>+ Find the most used domains in questions.</p>
    <p>+ Calculate the total points of User by day.</p>
    <p>+ Calculate the total number of points that User has achieved in a period of </p>
    <p>+ Find questions with many answers.</p>
    <p>+ Find active users.</p>
- To practice and learn about some tool to handle big data

## Technology and skill
- Apache Hadoop, Apache Spark, Apache Kafka, Apache Airflow
- MongoDB
