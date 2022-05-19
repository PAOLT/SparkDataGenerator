# Databricks notebook source
# MAGIC %md
# MAGIC Module to run queries

# COMMAND ----------

import time
import numpy as np
from pyspark.sql.functions import *

# COMMAND ----------

class QueryRun():
    def __init__(self, query_name, table_name, return_count = 'count', projection_columns='*', limit=None):
        '''
        A class representing a benchmarking session for a specific query
        INPUTS:
          query_name: a given name for the quey
          table_name: a delta table name
          return_count: type of query: valid arguments are 'no', 'count', 'distinct' (default: 'count')
          projection_columns: a list of projection columns. Ignored if return_count is 'count'
          limit: the number of records to return
        '''

        self.table = table_name
        self.return_count=return_count
        self.limit_clause = f"limit {limit}" if limit and return_count == 'no' else ""
        projection_columns = ", ".join(projection_columns) if type(projection_columns)==list else projection_columns
        if return_count == 'no':
          self.projection_clause = projection_columns
        elif return_count == 'distinct':
          self.projection_clause = f"distinct count({projection_columns}) as num_records"
        else:
          self.projection_clause = "count(*) as num_records"
          
        self.query_name = query_name
        self.base_statement = f"select {self.projection_clause} from {self.table}"
        self.times = []

    def get_statistics(self):
        '''
          Calculate statistics of the benchmarking session
          OUTPUT:
            a dictionary of statistics
        '''
        statistics = {}
        statistics["query"] = self.query_name
        statistics["min"] = np.around(np.min(self.times)/10**9, 4)
        statistics["max"] = np.around(np.max(self.times)/10**9, 4)
        statistics["mean"] = np.around(np.mean(self.times)/10**9, 4)
        statistics["median"] = np.around(np.median(self.times)/10**9, 4)
        statistics["std"] = np.around(np.std(self.times)/10**9, 4)
        return statistics
    
    def get_times(self):
		'''
			Returns the list of execution times in seconds for the benchmarking session
		'''
      return [np.around(t/10**9, 4) for t in self.times]

    def print_report(self):
        '''
          Print statistics of the test session
        '''

        statistics = self.get_statistics()
        print(f"Mean: {statistics['mean']} sec")
        print(f"Median: {statistics['median']} sec")
        print(f"Min: {statistics['min']} sec")
        print(f"Max: {statistics['max']} sec")
        print(f"Standard deviation: {statistics['std']} sec")

    def get_sql_statement(self, predicate):
        '''
        Compile a simple SQL statement, i.e., <select count(*) from table where predicates>
        INPUTS:
          predicate: a dictionary to generate a conjunction of predicates in the where clause
        OUTPUT:
          A SQL statement
        '''
        
        predicate_lit = ""
        for k, v in predicate.items():
            lit = f"{k} == '{v}'"
            predicate_lit = f"{predicate_lit} and {lit}" if predicate_lit != "" else f"where {lit}"
        return f"{self.base_statement} {predicate_lit} {self.limit_clause}".strip()

    def run_sql_statement(self, predicate={}):
        '''
          Function to run a sql statement from a table name and a dictionary represnting predicates 
          INPUTS:
            predicate: a dictionary to generate a conjunction of predicates in the where clause
          OUTPUT:
            The count of records returned
            The execution time in seconds
        '''
        statement = self.get_sql_statement(predicate)
        print(f"Running <{statement}>", end="")
        t0 = time.time_ns()        
        if self.return_count != 'no':
          count = spark.sql(statement).collect()[0]["num_records"]
        else:
          count = len(spark.sql(statement).collect())
        t1 = time.time_ns()
        exec_time = t1-t0
        self.times.append(exec_time)
        print(
            f"\t>>> Query returned {count} records in {exec_time/10**9} seconds")
        return count, exec_time
      
    def __repr__(self):
        return f"{self.base_statement} where ... {self.limit_clause}"
      
    def __str__(self):
        return self.__repr__()


# COMMAND ----------

help(QueryRun)
