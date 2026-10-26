# Hadoop worker image: the stock Hadoop image plus a portable Python, so PySpark
# drivers (cluster mode) and Python code in executors can run on the NodeManagers.
# Production clusters build every worker from one image like this, so all nodes
# run the same Python.
#
# The Python is a python-build-standalone release, which runs on old Linux such as
# this image's CentOS 7. Pass its unpacked folder as the "python" build context
# (see docker-compose.yml, PYTHON_DIST).
FROM apache/hadoop:3.4.1
COPY --from=python / /opt/python
ENV PYSPARK_PYTHON=/opt/python/bin/python3
