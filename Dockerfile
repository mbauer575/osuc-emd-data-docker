FROM python:3.10-slim-buster

# Install necessary tools
RUN apt-get update \
    && apt-get install -y gnupg2 curl unixodbc-dev

# Add Microsoft repository for SQL Server ODBC Driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Install SQL Server ODBC Driver
RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Copy requirements.txt
COPY requirements.txt /

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
ADD . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "function_app.py"]