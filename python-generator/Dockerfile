FROM python:3.12.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .

RUN python -m pip install \
    --no-cache-dir \
    --requirement requirements.txt

COPY src ./src
COPY tests ./tests

RUN addgroup --system orderflow \
    && adduser --system \
        --ingroup orderflow \
        --home /app \
        orderflow \
    && mkdir -p /app/output \
    && chown -R orderflow:orderflow /app

USER orderflow

ENTRYPOINT ["python", "-m", "orderflow_generator.batch_main"]

CMD ["--orders", "10", "--seed", "42", "--delay-probability", "0.20"]