FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY deploy/elastic-dashboard/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Only the standalone service is needed; no torch, Kubernetes SDK or full repo.
COPY dlrover/__init__.py /app/dlrover/__init__.py
COPY dlrover/python/__init__.py /app/dlrover/python/__init__.py
COPY dlrover/python/common/__init__.py dlrover/python/common/log.py dlrover/python/common/constants.py /app/dlrover/python/common/
COPY dlrover/dashboard/__init__.py /app/dlrover/dashboard/__init__.py
COPY dlrover/dashboard/elastic/*.py dlrover/dashboard/elastic/schema.sql /app/dlrover/dashboard/elastic/

RUN mkdir -p /data/logs/elastic-dashboard \
    && chown 10001:10001 /data/logs/elastic-dashboard
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["python", "-m", "dlrover.dashboard.elastic"]
CMD ["--host", "0.0.0.0", "--port", "8080"]
