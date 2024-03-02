# Ingest country data and derive insights


## What it should be able to do:-

Comparative Analysis: Provide tools for citizens to compare data across regions, sectors, or time periods to identify disparities and trends.
Data Visualization: Use interactive charts, graphs, and maps to make complex data more understandable and accessible.
User-Friendly Search and Filters: Allow users to easily find specific data points and customize their information exploration.
Trends Over Time: Highlight historical trends to show how policies and governance have evolved and their impacts.
Fact-Checking and Accountability: Enable users to cross-reference official data with other reliable sources to ensure accuracy and transparency.


## To Do:-

1. Add SCD to catalog changes (No need to add to metadata as it is tracked by git)
2. Extract root catalog aggregations and make a report by state/dept etc based on the aggregations, see how it changes - EDA notebook
3. Analyze trending dataset changes over time and see why they're trending - EDA notebook
4. Store all resources too (from api.data.gov and data.gov)- Compare with Catalog data and see which is better for EDA
5. New source - https://directory.apisetu.gov.in/



## Run locally using docker

docker build -t spark-jupyter .

docker run -d --name spark-jupyter-container \
  -p 8888:8888 \
  -p 4040:4040 \
  -v $(pwd):/home/jovyan/work \
  spark-jupyter


# Run locally on MacOS

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

<!-- export PYSPARK_PYTHON=/Users/subhashpeshwa/miniconda/bin/python
export PYSPARK_DRIVER_PYTHON='jupyter'
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port=8888' -->


For the system Java wrappers to find this JDK, symlink it with
  sudo ln -sfn /opt/homebrew/opt/openjdk/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk

If you need to have openjdk first in your PATH, run:
  echo 'export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"' >> ~/.zshrc

For compilers to find openjdk you may need to set:
  export CPPFLAGS="-I/opt/homebrew/opt/openjdk/include"


export PYSPARK_PYTHON=/usr/bin/python3
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home/
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'


<!-- pyspark --packages io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.pyspark.driver.python=/Users/subhashpeshwa/miniconda/bin/python" -->
Local Mode: pyspark --packages io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.pyspark.driver.python=/usr/bin/python3"
Cluster Mode: 

python3 -m notebook

/Users/subhashpeshwa/Library/Python/3.9/bin

