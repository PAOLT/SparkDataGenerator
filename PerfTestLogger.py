# Databricks notebook source
from azureml.core import Experiment, Run, Workspace
from azureml.core.authentication import InteractiveLoginAuthentication
import mlflow

# COMMAND ----------

class PerfTestLogger():
	'''
		A generic class to log benchmarking experiments
	'''
  def __init__(self):
    pass
    
  def start_run(self, name=None):
    '''
      Start a named run within the active experiment
	  INPUT:
		name: name of the run
    '''
    pass
  
  def log_tags(self, tags):
    '''
      Log a set of tags
      INPUTS:
        tags: a dictionary of tags
    '''
    pass
  
  def log_results(self, metric_name, metric_data):
    '''
      Log a set of metric data points (i.e., a chart)
      INPUTS:
        metric_name: a metric name
        metric_data: a list of values
    '''
    pass

  
  def complete_run(self):
    '''
      Close the experiment
    '''
    pass

# COMMAND ----------

class MLFlowLogger(PerfTestLogger):
	'''
	Log benchmarking experiments to MLFlow in DataBricks
	'''
  def __init__(self, experiment_name):
    '''
      Constructor.
      INPUTS:
        experiment_name: the name of the experiment
    '''
    mlflow.set_experiment(f"/Shared/Runs/{experiment_name}")
    
  def start_run(self, name=None):
    '''
      Start a named run within the active experiment
	  INPUT:
		name: name of the run
    '''
    mlflow.start_run()
  
  def log_tags(self, tags):
    '''
      Log a set of tags
      INPUTS:
        tags: a dictionary of tags
    '''
    for tag_name, tag_value in tags.items():
      mlflow.set_tag(tag_name, tag_value)

  
  def log_results(self, metric_name, metric_data):
    '''
      Log a set of metric results
      INPUTS:
        metric_name: a metric name
        metric_data: a list of values
    '''
    for step, metric_value in enumerate(metric_data):
      mlflow.log_metric(metric_name, metric_value, step)

  
  def complete_run(self):
    '''
      Close the experiment
    '''
    mlflow.end_run()

# COMMAND ----------

class AMLLogger(PerfTestLogger):
	'''
		Log benchmarking experiments to AML
	'''
  def __init__(self, experiment_name, tenant_id, subscription_id, resource_group, workspace_name):
    '''
      Constructor
      INPUTS:
        experiment_name: an experiment name
        tenant_id:
        subscription_id:
        resource_group: 
        workspace_name: the name of an existing AML workspace
    '''
    interactive_auth = InteractiveLoginAuthentication(tenant_id=tenant_id) if tenant_id else None
    self.ws = Workspace(subscription_id = subscription_id, resource_group = resource_group, workspace_name = workspace_name, auth=interactive_auth)
    self.experiment = Experiment(self.ws, experiment_name)
    
  def start_run(self, name=None):
	'''
      Start a named run within the active experiment
	  INPUT:
		name: name of the run
    '''
    self.run = self.experiment.start_logging(display_name = name, outputs=None, snapshot_directory=None)
  
  def log_tags(self, tags):
    '''
      Log a set of tags
      INPUTS:
        tags: a dictionary of tags
    '''
    for k, v in tags.items():
      self.run.tag(k, v)
  
  def log_results(self, metric_name, metric_data):
    '''
      Log a set of metric results
      INPUTS:
        data: a dictionary of results
    '''
    self.run.log_list(metric_name, metric_data)
  
  def log_row(self, row_data):
    '''
      Log a row of statistics for a query
      INPUTS:
        row_name: the name of the query
        row_data: a dictionary with statistics
    '''
    self.run.log_row("Summary", **row_data)
  
  def complete_run(self):
    '''
      Close the experiment
    '''
    self.run.complete()
  
  def cancel_run(self):
    '''
      Cancel the experiment
    '''
    self.run.cancel()
    
  def get_url(self):
	'''
		Returns the URL of the run in the AML portal
	'''
    return self.run.get_portal_url()

# COMMAND ----------

help(AMLLogger)

# COMMAND ----------

help(MLFlowLogger)
