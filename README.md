# NER Pipeline with NLTK Integration

## Overview

This repository contains a **Named Entity Recognition (NER) pipeline** leveraging **NLTK (Natural Language Toolkit)** resources for advanced text processing.

## Feature

- **Webhook Integration**: Triggered by incoming metadata for unstructured data.
- **File Retrieval**: Supports various file formats:
  - **Unstructured**: Text, Docs, PDFs from MinIO storage.
  - **Structured**: JSON, CSV, XLSX via API or OMD pipeline.
- **PII Data Processing**: Utilizes the PII Scanner library to extract and process sensitive information.
- **Data Mapping**: Processes structured and unstructured data, mapping it into predefined schemas.
- **Data Storage**: Inserts or updates data across multiple ClickHouse tables.
- **API Support**: Facilitates seamless interaction with the frontend dashboard.

## Requirements

- Python 3.11 (or higher)
- NLTK library
- Pandas
- FastAPI
- Uvicorn
- ClickHouse (for data storage)
- Docker (for containerization)

## Installation

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd <your-repository-directory>
```

### 2. Set Up a Virtual Environment
```bash
python3.11 -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Download Required NLTK Resources
Run the following script in your Python environment:
```python
import nltk

nltk.download('punkt')  # Tokenizer model
nltk.download('stopwords')  # Common stop words
nltk.download('averaged_perceptron_tagger')  # Part-of-speech tagging model
nltk.download('wordnet')  # WordNet lexical database
nltk.download('omw')  # Open Multilingual WordNet
nltk.download('movie_reviews')  # Movie review corpus
nltk.download('reuters')  # Reuters corpus
nltk.download('treebank')  # Annotated treebank corpus
nltk.download('universal_tagset')  # Universal part-of-speech tagset
```

---

## Running the Application

### Using Uvicorn

To start the FastAPI application locally:
```bash
uvicorn main:app --reload
```

- The application will be accessible at `http://localhost:8000`.
- Use `--reload` for automatic server restarts during development.

### Using Docker

1. Build and Run the Docker Image:
```bash
docker compose up --build
```

- The application will be accessible at `http://localhost:8002`.
- Adjust port settings in the `docker-compose.yml` file as needed.

---

## Directory Structure

- **app/**: Contains the FastAPI application code.
  - **routers/**: API routes, including:
    - `ner_router`: Processes structured data.
    - `omd_router`: Handles metadata from the OMD pipeline.
    - `unstructured_ner_router`: Processes unstructured data via webhook and MinIO.
  - **utils/**: Utility files for additional project functionalities.
  - **logs/**: Logs for monitoring and debugging.
  - **constants/**: Configuration constants used across the project.
  - **middleware/**: Middleware components for request/response handling.
  - **views/**: Views for processing structured and unstructured data tables.

- **migration/**: Contains SQL files for database migrations.
- **migrate.py**: Script to handle database migrations, including execution of SQL files.
- **Dockerfile**: Instructions for building the Docker image.
- **docker-compose.yml**: Configuration for Docker Compose.
- **requirements.txt**: Python dependencies.

---

## Workflow

### Unstructured Data Processing

1. Receive a webhook with metadata.
2. Fetch files (Text, Docs, PDFs) from MinIO storage.
3. Process files using the PII Scanner library.
4. Map and arrange data into structured formats.
5. Update or insert data into ClickHouse tables:
   - `unstructured_ner_result`: Specific to unstructured data.

### Structured Data Processing

1. Data is routed directly from the OMD pipeline or API.
2. Process the data and map it into predefined schemas.
3. Update or insert into ClickHouse tables:
   - `column_ner` (results)
   - `profiler_time_series`
   - `profiler_metadata`
   - `dbservice_entity`
   - `dbservice_schema_entity`
   - `dbservice_meta_info`
   - `table_entity`

