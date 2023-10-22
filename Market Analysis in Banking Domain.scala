//Import necessary libraries 
val sqlContext = spark.sqlContext

//1.Load data and create a Spark data frame.
val df = spark.read.options(Map("inferSchema"->"true","delimiter"->";","header"->"true")).csv("dataset_bank.csv")

//Create a temporary table
df.registerTempTable("bank")
sqlContext.sql("SELECT * FROM bank").show() // show the table 

//2.Give marketing success rate (No. of people subscribed / total no. of entries).
sqlContext.sql("SELECT COUNT(*) *100.0/(SELECT COUNT(*) FROM bank) AS Success_Rate FROM bank WHERE y='yes'").show()

//3.Give marketing failure rate.
sqlContext.sql("SELECT COUNT(*) *100.0/(SELECT COUNT(*) FROM bank) AS Failur_Rate FROM bank WHERE y='no'").show()

//4.Give the maximum, mean, and minimum age of the average targeted customer.
df.select(avg("age")).show()
df.select(min("age")).show()
df.select(max("age")).show()

//5.Check the quality of customers by checking average balance, median balance of customers.
df.select(avg("balance")).show()
sqlContext.sql("SELECT percentile_approx(balance, 0.5) FROM bank").show()

//6.Check if age matters in marketing subscription for deposit.
df.filter("y='yes'").groupBy("age").agg(count("age").alias("total")).sort(col("total").desc).show()

//7.Check if marital status mattered for a subscription to deposit.
df.filter("y='yes'").groupBy("marital").agg(count("marital").alias("total")).sort(col("total").desc).show()

//8.Check if age and marital status together mattered for a subscription to deposit scheme.
df.filter("y='yes'").groupBy("age","marital").agg(count("*").alias("total")).sort(col("total").desc).show()

//9.	Do feature engineering for the bank and find the right age effect on the campaign.
// Young adult (18-25 age), Adult (26-44 age), Middle-age (45-59 age), Old age (60 <age)  
val ageRDD = sqlContext.udf.register("ageRDD",(age:Int) => { if (age <25) "Young Adult" else if (age > 26 && age <= 44) "Middle Aged" else "Old" })
val df2 = df.withColumn("age",ageRDD(df("age")))
df2.filter("y='yes'").groupBy("age").agg(count("age").alias("total")).sort(col("total").desc).show()

