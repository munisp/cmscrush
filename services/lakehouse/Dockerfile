FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir ".[runtime]"
ENV PYTHONPATH=/app/src
USER 65532:65532
ENTRYPOINT ["python", "-m", "crush_lakehouse.jobs"]
# Production images pin the matching Spark, Flink, Sedona, Delta Lake, and Ray versions
# after compatibility validation; the broad bounds in pyproject are development constraints.
