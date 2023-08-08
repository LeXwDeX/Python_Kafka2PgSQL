# Use an official Python runtime as a parent image
FROM python:3.10.12

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
ADD . /app

# Install any needed packages specified in requirements.txt
RUN apt-get update && apt-get install -y libpq-dev librdkafka-dev gcc
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Create directories for config and log files
RUN mkdir /app/config
RUN mkdir /app/log

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME kafka2pgsql

# Run app.py when the container launches
CMD ["python", "main.py"]
