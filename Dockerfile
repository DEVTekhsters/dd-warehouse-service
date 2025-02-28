# Use a smaller base image
FROM python:3.10-slim as builder

# Set working directory
WORKDIR /usr/src/app

# Install system dependencies (Tesseract, OpenGL, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr libgl1-mesa-glx libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Set environment variable for NLTK data
ENV NLTK_DATA="/usr/src/app/nltk_data"

# Create the NLTK data directory
RUN mkdir -p $NLTK_DATA

# Copy dependency file first (to optimize caching)
COPY requirements.txt ./

# Install dependencies in a single step
RUN pip install --no-cache-dir -r requirements.txt spacy nltk

# Download required Spacy models in one step
RUN python -m spacy download en_core_web_sm \
    && python -m spacy download en_core_web_md \
    && python -m spacy download en_core_web_lg

# Download all required NLTK datasets in one command
RUN python -c "import nltk; nltk.download(['punkt', 'punkt_tab', 'stopwords', 'averaged_perceptron_tagger_eng', 'maxent_ne_chunker_tab'], download_dir='$NLTK_DATA')"

# Copy application files
COPY . .

# Expose FastAPI default port
EXPOSE 8000

# Command to run the FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
