FROM python:3.10-slim-buster

COPY requirements.txt /

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
ADD . /app

# Install unixODBC
RUN apt-get update && apt-get install -y unixodbc-dev

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "function_app.py"]