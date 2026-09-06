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
FROM python:3.13-alpine AS builder

WORKDIR /app

# Install build tools required for compiling C/C++ extensions (pyarrow, pandas) on Alpine
RUN apk add --no-cache \
    build-base \
    g++ \
    cmake \
    musl-dev \
    linux-headers

# Copy and install dependencies into a localized prefix folder
COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

# Stage 2: Final runtime stage
FROM python:3.13-alpine

WORKDIR /usr/src/app

# awswrangler (via pyarrow/numpy) requires libstdc++ to run C++ extensions on Alpine
RUN apk add --no-cache libstdc++

# Copy installed site-packages from the builder stage
COPY --from=builder /install /usr/local

# Copy application source code
COPY . .

# Run application
CMD ["python", "app.py"]