# Introduction 

This repository holds experimentation notebooks to support ad-hoc query performance benchmarking over arbitrary datasets in Spark Delta Tables.

The code has been tested with Azure DataBricks. 

The following notebooks are provided:
- `./DataGenerator.py` holds the `DataGenerator` helper class to generate data.
- `./QueryRun.py` holds the `QueryRun` helper class to run queries with parametric where clauses, and measure performance. Queries are templatized over `select <projection_columns> from <table> where <conjunction>`, `select count(*) as num_records from <table> where <conjunction>` or `select distinct count (<projection_columns>) from <table> where <conjunction>`.
- `./PerfTestLogger.py` holds the `PerfTestLogger` helper class to log experiments to Azure ML or MLFlow.
- `./Test-Example.py` shows an example notebook that uses the above classes to run an end to end benchmarking session. 

# Getting Started

Using the classes is very simple: import the notebooks in your own Spark environment and start writing your benchmarking notebook similar to `./Test-Example.py`. Consider that the notebooks have been edited in Azure DataBricks, thus different systems might require some adaptations.

# Contribute
If you wish to contribute, clone this repository and publish your PR.