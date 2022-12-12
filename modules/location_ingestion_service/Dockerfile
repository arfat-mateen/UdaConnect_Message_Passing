FROM python:3.7-alpine

WORKDIR .

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

EXPOSE 5006

COPY . .
CMD ["python", "location_ingestion_service.py"]