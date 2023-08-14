# Ingest country data and derive insights

brew install openjdk@17
brew install apache-spark
spark-shell --version

conda create --name delta_env python=3.10 -y
conda activate delta_env

conda install pyspark
conda install -c condaforge delta-spark
conda install -c condaforge mimesis

conda env export > environment.yaml
conda env create --file environment.yaml
conda env list
conda deactivate

export PYSPARK_PYTHON=/Users/subhashpeshwa/miniconda/bin/python
pyspark --packages io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.pyspark.driver.python=/Users/subhashpeshwa/miniconda/bin/python"
python3 -m jupyter notebook

## What it should be able to do:-

Comparative Analysis: Provide tools for citizens to compare data across regions, sectors, or time periods to identify disparities and trends.
Data Visualization: Use interactive charts, graphs, and maps to make complex data more understandable and accessible.
User-Friendly Search and Filters: Allow users to easily find specific data points and customize their information exploration.
Trends Over Time: Highlight historical trends to show how policies and governance have evolved and their impacts.
Fact-Checking and Accountability: Enable users to cross-reference official data with other reliable sources to ensure accuracy and transparency.

