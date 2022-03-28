# syntax=docker/dockerfile:1
FROM python:3.10-slim
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DB_USER=gcn
ENV DB_PASSWORD=gcn
ENV DB_HOST=db
ENV DATABASE=gcn
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt
COPY . /code/
CMD ["python", "-m", "main", "-v", "-sh", "-np"]
