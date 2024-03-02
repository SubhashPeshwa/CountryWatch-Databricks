# Use a base image with Spark and Jupyter pre-installed
FROM jupyter/pyspark-notebook:latest

# Expose Jupyter Notebook port
EXPOSE 8888

COPY ./requirements.txt ./requirements.txt
# Install any additional Python packages or configurations if needed
RUN pip install -r ./requirements.txt

# Start Jupyter Notebook
CMD ["start-notebook.sh"]