# Use an official Python runtime as a parent image
#FROM python:3.9
# Set the working directory in the container
#WORKDIR /usr/src/app
# Copy the current directory contents into the container at /usr/src/app
#COPY . .
# Install any needed packages specified in requirements.txt
#RUN pip install --no-cache-dir -r requirements.txt
# Run app.py when the container launches
#CMD ["python", "app.py"]

# Stage 1: Build stage
FROM python:3.9-alpine AS builder

WORKDIR /app

# Install build dependencies required for compiling wheel extensions
RUN apk add --no-cache build-base g++ musl-dev

COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

# Stage 2: Final runtime stage
FROM python:3.9-alpine

WORKDIR /app

# Copy installed site-packages from builder
COPY --from=builder /install /usr/local

COPY . .

CMD ["python", "app.py"]