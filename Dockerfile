FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py extract_pdfs.py migrate_to_postgres.py ./

# Default: run the PDF extractor. Override for scraping:
#   docker compose run --rm scraper python main.py
CMD ["python", "extract_pdfs.py"]
