# Databricks notebook source
# MAGIC %md
# MAGIC Module to generate large historical data to support benchmarking

# COMMAND ----------

import datetime
import random
import uuid
from dataclasses import dataclass
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------


def addTimeFun(dt, d, h, m, s):
    D = datetime.timedelta(days=d)
    H = datetime.timedelta(hours=h)
    M = datetime.timedelta(minutes=m)
    S = datetime.timedelta(seconds=s)
    return (dt+D+H+M+S)

addTime = udf(lambda dt, d, h, m, s: addTimeFun(
    dt, d, h, m, s), TimestampType())

# COMMAND ----------

def getGUID():
	'''
		Returns a GUID
	'''
    return str(uuid.uuid4())

getTransactionGUID = udf(getGUID, StringType())

# COMMAND ----------


def sample_value_fun(lst):
	'''
		Sample a single value from a list of values
	'''
  return str(random.sample(lst, 1)[0])

sample_value = udf(lambda lst: sample_value_fun(lst), StringType())

# COMMAND ----------

@dataclass
class instruction:
  '''
    Class to represent an instruction used in a program to generate data
    
    INPUT:
      column_name: the name of a column to generate
      method: can be 'explode', 'sample', 'id' or 'guid'. 'explode' will explode the column over all of its values. 'sample' will randomly sample a single value. 'id' will generate an ID column. 'guid' will generate a GUID column
      column_values: a list of possible values for the column. Only string values are supported.
  '''
  column_name: str
  method: str
  column_values: list

# COMMAND ----------

class DataGenerator():
    def __init__(self, generation_code):
        '''
        Instantiate a DataGenerator object, that mantains a spark dataframe of transactions.

        INPUT:
        generation_code: A list of instruction objects used to generate a dataframe by executing instructions serially
        '''
        self.data = None
        self.generation_code = generation_code

        # used for lineage only
        self.num_days = []
        self.num_hours = []
        self.num_minutes = []
        self.num_seconds = []
        self.rps = []
        self.start_date = []

    def generateData(self, num_days, num_hours, num_minutes, num_seconds, rps, start_date, override_values=None):
        '''
        Generates a new data period.
        INPUTS:
            num_days: the number of days of data to generate
            num_hours:  the number of business hours per day
            num_minutes:  the number of minutes per hour of data to generate
            num_seconds:  the number of seconds per minute of data to generate (only applied if num_minutes = 60)
            rps:  the number of requests per second of data to generate (only applied if num_seconds = 60)
            start_date: the date to start from, i..e, a tuple of (year, month, day)
            override_values: a dictionary of column_name to values to override values in the original 
        OUTPUTS:
            a Spark dataframe
            a dictionary with lineage information
        '''

        # base date-time
        dt = datetime.datetime(start_date[0], start_date[1], start_date[2])

        # days/hours/minutes/seconds/rps per month to generate
        days = [x for x in range(num_days)]
        hours = [x for x in range(num_hours)]
        mins = [x for x in range(num_minutes)]
        secs = [x for x in range(num_seconds)] if num_minutes == 60 else [0]
        rps = [x for x in range(rps)] if num_seconds == 60 else [0]

        # Generate a Pandas dataframe
        record = [days, hours, mins, secs, rps]
        columns = ["Day", "Hour", "Minute", "Second", "RPS"]

        data = [tuple(record)]
        pdf = pd.DataFrame(data, columns=columns)
        pdf = pdf.explode("Day", ignore_index=True)
        pdf = pdf.explode("Hour", ignore_index=True)
        pdf = pdf.explode("Minute", ignore_index=True)
        pdf = pdf.explode("Second", ignore_index=True)
        
        # Create a Spark dataframe from the Pandas dataframe
        columns_to_return = []
        df = spark.createDataFrame(data=pdf)

        # Generte transactions by exploding over RPS
        df = df.withColumn("RPS", explode(col("RPS")))
      
        # Add transactions timestamp and date
        df = df.withColumn("TimeStamp", addTime(lit(dt), col("Day"), col(
            "Hour"), col("Minute"), col("Second")).cast(TimestampType()))
        columns_to_return.append("TimeStamp")

        df = df.withColumn("TheDate", col("TimeStamp").cast(DateType()))
        columns_to_return.append("TheDate")
        
        for generation_instruction in self.generation_code:
          
          assert (generation_instruction.method in ['explode', 'sample', 'guid', 'id'])
          columns_to_return.append(generation_instruction.column_name)
          
          if override_values and generation_instruction.column_name in override_values.keys():
            column_values = override_values[generation_instruction.column_name]
          else:
            column_values = generation_instruction.column_values
          
          msg = f"Executing command <{generation_instruction.method}> on column <{generation_instruction.column_name}> "
          msg = msg +  f"with values {column_values}" if column_values else msg
          print(msg)
          
          if generation_instruction.method == 'explode':
            column_values_array = array([lit(v) for v in column_values])
            df = df.withColumn(generation_instruction.column_name, column_values_array)
            df = df.withColumn(generation_instruction.column_name, explode(generation_instruction.column_name))
          
          if generation_instruction.method == 'sample':
            column_values_array = array([lit(v) for v in column_values])
            df = df.withColumn(generation_instruction.column_name, column_values_array)
            df = df.withColumn(generation_instruction.column_name, sample_value(generation_instruction.column_name))
          
          if generation_instruction.method == 'guid':
            df = df.withColumn(generation_instruction.column_name, getTransactionGUID()) 
          
          if generation_instruction.method == 'id':
            df = df.withColumn(generation_instruction.column_name, monotonically_increasing_id())
            
        lineage = {'num_days': num_days, 'num_hours': num_hours, 'num_minutes': num_minutes,
                   'num_seconds': num_seconds, 'rps': rps, 'start_date': start_date}

        return df.select(columns_to_return), lineage

    def addData(self, df, lineage):
      '''
      Add a dataframe to a DataGenerator object.
      INPUT
        df: a valid dataframe, coherent with the DataGenerator object (same schema)
        lineage: lineage information returned by the generateData method
      OUTPUT:
        the new Spark dataframe managed by the DataGenerator object
      '''
      # manage lineage information
      self.num_days.append(lineage['num_days'])
      self.num_hours.append(lineage['num_hours'])
      self.num_minutes.append(lineage['num_minutes'])
      self.num_seconds.append(lineage['num_seconds'])
      self.rps.append(lineage['rps'])
      self.start_date.append(lineage['start_date'])

      # merge data
      self.data = self.data.union(df) if self.data else df
      return self.data

    def getData(self):
      '''
        Returns the Spark dataframe managed by the DataGenerator object
      '''
      if self.data:
          return self.data
      else:
          return spark.createDataFrame(sc.emptyRDD(), StructType([]))

    def count(self):
      '''
      Returns the number of records in the Spark dataframe managed by the DataGenerator object
      '''
      if self.data:
          return self.data.count()
      else:
          return 0

    def writeData(self, name_or_path, partitions=[], sampling=None, format='delta', mode='overwrite'):
        '''
        Writes the Spark dataframe managed by the DataGenerator object
        INPUT:
          name_or_path: delta table name or file path.
          partitions: a valid list of columns names or a column name.
          sampling: a fraction to sample (between 0.0 and 1.0)
          format: a DataFrameWriter format
          mode: a DataFrameWriter mode
        '''

        data = self.data.sample(False, sampling, 42) if sampling else self.data

        if type(partitions) == str:
            partitions = [partitions]

        if format == 'delta':
            data.write.format('delta').partitionBy(partitions).mode(mode).saveAsTable(name_or_path)
        else:
            data.write.format(format).partitionBy(partitions).mode(mode).save(name_or_path)

    def __str__(self):
        if self.data:
            return self.data.__repr__()
        else:
            return self.__repr__()

    def __repr__(self):
        repr = "### Generation code:\n"
        repr = repr + "\n".join([str(i) for i in self.generation_code])
        repr = repr + "\n\n### Data generations:"
        for num_days, num_hours, num_minutes, num_seconds, rps, start_date in zip(self.num_days, self.num_hours, self.num_minutes, self.num_seconds, self.rps, self.start_date):
            repr = repr + \
                f"\ngenerateData({num_days}, {num_hours}, {num_minutes}, {num_seconds}, {rps}, {start_date})"
        return repr

# COMMAND ----------

help(DataGenerator)
