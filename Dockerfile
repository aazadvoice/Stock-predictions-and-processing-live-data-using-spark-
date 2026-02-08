FROM jupyter/pyspark-notebook

# Copy requirements
COPY requirements.txt /tmp/

# Install pinned dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

