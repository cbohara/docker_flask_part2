Repo for Udemy course Taming Big Data with Apache Spark and Python

https://www.udemy.com/taming-big-data-with-apache-spark-hands-on/learn/v4/content

### RDD = an abstraction for giant dataset
- can transform RDD into another RDD
- or perform actions on the RDD to get results

### Spark context (sc object)
- gives you methods you need to create an RDD
- use sc.textFile() to create an RDD from a file

### RDD transformations
- map = transform each value (1-to-1 relationship) in original RDD based on a lambda function to a new value in resulting RDD 
- flatmap = same as map except 1 value in original RDD can result in multiple values in generated RDD (not 1-to-1)
