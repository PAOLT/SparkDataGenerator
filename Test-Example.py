# Databricks notebook source
# MAGIC %md
# MAGIC # Running ad-hoc query performance benchmarking on Spark Delta Tables
# MAGIC 
# MAGIC We defined some classes to help on this scenario, and this notebooks shows how to use those classes.
# MAGIC 
# MAGIC - **DataGenerator** is a class to generate data as a delta table in Spark
# MAGIC - **QueryRun** is a class to run queries
# MAGIC - **PerfTestLogger** is a class to log results to AML or MLFlow
# MAGIC 
# MAGIC Classes are defined in notebooks as experimental code, and can be easily modified.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate historical data with the DataGenerator class

# COMMAND ----------

# MAGIC %md
# MAGIC The DataGenerator class is based on a simple language to write an high level data generation program. The language let to:
# MAGIC 
# MAGIC - generate a column whose value is sampled from a list of values
# MAGIC - explode a column whose value is an array of values
# MAGIC - generate a numeric ID
# MAGIC - generate a GUID
# MAGIC 
# MAGIC The DataGenerator stores a Spark dataframe, and let to write it as a Delta Table.

# COMMAND ----------

# MAGIC %run ./DataGenerator

# COMMAND ----------

# MAGIC %md
# MAGIC Declare a list of instruction objects that defines the high level program for generating the dataframe. We start with a dataframe of time-stamps. Each instruction object generates a column in the dataframe, by either sampling values, exploding values, generating an ID or generating a GUID.

# COMMAND ----------

generation_code = []
generation_code.append(instruction("origin", "sample", ["System-1", "System-2"]))
generation_code.append(instruction('user', "sample", ["User1", "User2", "User3"]))
generation_code.append(instruction('TransactionID', "id", None))
generation_code.append(instruction('TransactionGUID', "guid", None))
generation_code.append(instruction('docType', "explode", ["Search", "Provider1-Offer", "Provider2-Offer", "Provider3-Offer"]))
generation_code.append(instruction('Category', "sample", ["Cat1", "Cat2"]))
generation_code

# COMMAND ----------

# MAGIC %md
# MAGIC We use the data generation program to instantiate a DataGenerator object

# COMMAND ----------

dataGen = DataGenerator(generation_code)
dataGen

# COMMAND ----------

# MAGIC %md
# MAGIC Data generation actually happen by exploiting a time span. Here we are considering a time span of 2 days from 2021/1/1, and 3 records per second. Each record corresponds to a business transaction.

# COMMAND ----------

df1, lineage1 = dataGen.generateData(
    num_days=2, num_hours=2, num_minutes=60, num_seconds=60, rps=3, start_date=(2021, 1, 1))
df1.count()

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC The generateData method into the DataGeneration object returns a Spark dataframe and some lineage information, useful to annotate the data with the actions that produced it.

# COMMAND ----------

lineage1

# COMMAND ----------

# MAGIC %md
# MAGIC The dataframe is now stored into the dataGen object

# COMMAND ----------

dataGen.count()

# COMMAND ----------

dataGen.addData(df1, lineage1)
dataGen.count()

# COMMAND ----------

dataGen

# COMMAND ----------

# MAGIC %md
# MAGIC Now we generate a second dataframe and add it to the dataGen object. Note that we are redefining the possible values for the column "origin". The opportunity to keep adding dataframes to a DataGenerator object lets to construct asymmetric dataframes.

# COMMAND ----------

df2, lineage2 = dataGen.generateData(
    num_days=2, num_hours=3, num_minutes=60, num_seconds=60, rps=3, start_date=(2021, 1, 4), override_values={'origin':['System-x', 'System-y']})
df2.count()

# COMMAND ----------

display(df2)

# COMMAND ----------

dataGen.addData(df2, lineage2)
dataGen.count()

# COMMAND ----------

# MAGIC %md
# MAGIC The DataGenerator class representation provides a lineage of actual data.

# COMMAND ----------

dataGen

# COMMAND ----------

# MAGIC %md
# MAGIC Let's write the data as a delta table, partitioning by date

# COMMAND ----------

dataGen.writeData(name_or_path="mini_table", partitions="TheDate")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL mini_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mini_table

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we construct a new table as a sampling of the previous table. This is useful to sample values to use in where clauses when benchmarking queries, because we want to avoid reading those values from the benchmark table. In fact, that would be like cheating from a benchmarking perspective: retrieving records that have been just retrieved a second before shouldn't be too difficult for a system!

# COMMAND ----------

num_test_queries = 50
count = dataGen.count()
fraction = num_test_queries/count
dataGen.writeData(name_or_path='mini_table_sampled', sampling=fraction)

# COMMAND ----------

df=sqlContext.table("mini_table_sampled")
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run queries with the PerfTest class

# COMMAND ----------

# MAGIC %md
# MAGIC The QueryRun class defines some parametric query templates, run them and capture execution times.

# COMMAND ----------

# MAGIC %run ./QueryRun

# COMMAND ----------

# MAGIC %md
# MAGIC We use the sampled table to pre-load some TransactionID and TransactionGUID values to use in query where clauses. The reason we get those values from a different table is because we want to avoid loading the benchmark table prior to run the query.

# COMMAND ----------

hist_data = sqlContext.table("mini_table_sampled").select(["TransactionId"]).collect()
hist_reference = [record["TransactionId"] for record in hist_data]
hist_reference_nn = len(hist_reference)
print(f"{hist_reference_nn} records sampled from dataframe")
hist_reference[:3]

# COMMAND ----------

hist_data2 = sqlContext.table("mini_table_sampled").select(["TransactionGUID"]).collect()
hist_reference2 = [record["TransactionGUID"] for record in hist_data2]
hist_reference2_nn = len(hist_reference2)
print(f"{hist_reference2_nn} records sampled from dataframe")
hist_reference2[:3]

# COMMAND ----------

# MAGIC %md
# MAGIC In case we had a stream writing to the benchmark table, we also need to sample some values for where clause. We have no streaming on this example, but we could use the streaming debug features to write in memory for sampling values before they are written to the table.

# COMMAND ----------

# debug_stream = streaming_df.writeStream.format("memory").queryName("mem_df").start()

# rt_reference_nn = 0
# while rt_reference_nn < hist_reference_nn:
#     time.sleep(10)
#     rt_reference = spark.sql("select * from mem_df").select(["TransactionId"]).distinct().collect()
#     rt_reference_nn = len(rt_reference)

# debug_stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC We now use the QueryRun class to run benchmark queries. The query is templetized into the class as a conjunctive query. Coun, distinct count and normal projections queries are supported, but the class can be extended easily. Specifically, for count and distinct count the system retrieve a number, that is, the net time to execute the query on the server is measured. For normal selections (e.g., select * from queries), the system actually retrieve the result on the client (the notebook on this case), and the execution time is affected. It depends on what you want to measure... anyway, a QueryRun object represents a single query to run.

# COMMAND ----------

QueryRun1 = QueryRun("SQL-1", "mini_table")
QueryRun1

# COMMAND ----------

QueryRun2 = QueryRun("SQL-2", "mini_table", return_count = 'no', projection_columns=['TimeStamp', 'user', 'TransactionID', 'docType'])
QueryRun2

# COMMAND ----------

# MAGIC %md
# MAGIC The run_sql_statement method runs the query. We do it on a cycle over the predicates values, and collect the results.

# COMMAND ----------

for i in range(hist_reference_nn):
  _, _ = QueryRun1.run_sql_statement({"TransactionId":hist_reference[i]})

# COMMAND ----------

QueryRun1.print_report()

# COMMAND ----------

# MAGIC %md
# MAGIC We do the same with the second query...

# COMMAND ----------

for i in range(hist_reference2_nn):
  _, _ = QueryRun2.run_sql_statement({"TransactionGUID":hist_reference2[i]})

# COMMAND ----------

QueryRun2.print_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log results
# MAGIC Results can be logged to Azure ML and Databricks MLFlow, by using the PerfTestLogger class.

# COMMAND ----------

# MAGIC %run ./PerfTestLogger

# COMMAND ----------

# MAGIC %md
# MAGIC We define some parameters to connect to AML, and some tags to log

# COMMAND ----------

# Init parameters to connect to an AML workspace
params = {}
params["experiment_name"] = ''
params["tenant_id"] = ''
params["subscription_id"] = ''
params["resource_group"] = ''
params["workspace_name"] = ''

# Some tags to log
tags = {}
tags["Table Name"] = "mini_table"
tags["Num records"] = sqlContext.table("mini_table").count()

# Run name
run_name = "run name4"

# COMMAND ----------

# MAGIC %md
# MAGIC Log with AML

# COMMAND ----------

aml_logger = AMLLogger(**params)
aml_logger.start_run(run_name)
aml_logger.log_tags(tags)

# log statistics
aml_logger.log_row(QueryRun1.get_statistics())
aml_logger.log_row(QueryRun2.get_statistics())

# log queries runs
aml_logger.log_results(QueryRun1.query_name, QueryRun1.get_times())
aml_logger.log_results(QueryRun2.query_name, QueryRun2.get_times())

aml_logger.complete_run()
# aml_logger.cancel_run()

# COMMAND ----------

aml_logger.get_url()

# COMMAND ----------

# MAGIC %md
# MAGIC Log with MLFlow

# COMMAND ----------

adb_logger = MLFlowLogger(experiment_name='databricks-2')
adb_logger.start_run()
adb_logger.log_tags(tags)

# log queries runs
adb_logger.log_results(QueryRun1.query_name, QueryRun1.get_times())
adb_logger.log_results(QueryRun2.query_name, QueryRun2.get_times())

adb_logger.complete_run()
