# SparkToPostgreSQL

This is code is based on this article and has small tweaks. We used to insert 230 million from Azure Databricks to Azure Postgres Hyperscale Citus and got this results:
	
>- With the traditional JDBC writer the job took 41 minutes<br>
>- Using the COPY command the job took **13** minutes

We intend to fine tune this use case and give futher details on the implementation (Databricks and Citus config)
Cheers.