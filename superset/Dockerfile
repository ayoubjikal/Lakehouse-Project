# In ./superset/Dockerfile
FROM apache/superset:latest

USER root

# Install Trino driver
RUN pip install trino sqlalchemy-trino


USER superset