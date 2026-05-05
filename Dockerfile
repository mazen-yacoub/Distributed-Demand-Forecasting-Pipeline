FROM apache/spark:3.5.0

USER root

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

USER spark
