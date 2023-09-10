# Use the latest Python runtime as the base image
FROM python:latest

# Set the working directory in the container to /usr/src/app
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . /usr/src/app

# Install the required libraries
RUN pip install --no-cache-dir numpy pandas

# Specify the command to run on container start
CMD ["python", "./test.py"]
