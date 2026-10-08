FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock

COPY . /app

RUN pip install --no-cache-dir --no-deps . \
    && useradd --create-home appuser

USER appuser

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
