# Use official Python image
FROM python:3.10

# Set work directory
WORKDIR /usr/src/app

# Install system dependencies (including OpenGL)
RUN apt-get update && apt-get install -y tesseract-ocr libgl1-mesa-glx libglib2.0-0 && rm -rf /var/lib/apt/lists/*  

# Set environment variable for NLTK data
ENV NLTK_DATA="/usr/src/app/nltk_data"

# Create the NLTK data directory
RUN mkdir -p $NLTK_DATA

# Install Python libraries
RUN pip install --no-cache-dir spacy nltk
 
# Download Spacy model
RUN python -m spacy download en_core_web_sm

RUN python -m spacy download en_core_web_md

RUN python -m spacy download en_core_web_lg

RUN python -m spacy download en_core_web_trf

# Download NLTK data
# RUN python -c "import nltk; nltk.download('popular', download_dir='$NLTK_DATA')"

RUN python -c "import nltk; nltk.download('punkt')"
RUN python -c "import nltk; nltk.download('punkt_tab')"
RUN python -c "import nltk; nltk.download('stopwords')"
RUN python -c "import nltk; nltk.download('averaged_perceptron_tagger_eng')"

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app files
COPY . .

# Expose FastAPI default port
EXPOSE 8000

# Command to run the FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
