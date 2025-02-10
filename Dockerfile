# Use official Python image
FROM python:3.10

# Set work directory
WORKDIR /usr/src/app

# Install system dependencies (including OpenGL)
RUN apt-get update && apt-get install -y tesseract-ocr libgl1-mesa-glx libglib2.0-0 && rm -rf /var/lib/apt/lists/*  
# Provides libGL.so.1 for OpenGL, Common dependency for many packages, including OpenCV, Clean up cache to reduce image size

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Install dependencies
# COPY requirements.txt ./
# RUN pip install -r requirements.txt -v
COPY req.txt ./
RUN pip install -r req.txt -v

# Copy the app files
COPY . .

# Expose FastAPI default port
EXPOSE 8000

# Command to run the FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
