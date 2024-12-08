# osuc-emd-data-docker
## Introduction
This repository contains the code and configuration for managing and collecting energy data at Oregon State University - Cascades campus. The program must be deployed within th 10. IP space to be able to access local EATON Power XPERT energy monitoring devices. 

## Features
- Python: The main programming language for data processing and management.
- Docker: Used for containerization to ensure consistent deployment.

## Installation
To clone and run this repository locally, follow these steps:

```
# Clone the repository
git clone https://github.com/mbauer575/osuc-emd-data-docker.git

# Navigate to the project directory
cd osuc-emd-data-docker

# Build the Docker image
docker build -t osuc-emd-data .

# Run the Docker container
docker run -d -p 8000:8000 osuc-emd-data
```

## Usage

Pulls Data from multiple energy monitors around campus and pushes that data to a Database hosted in AZURE. Modify .env file to point to energy monitors and SQL database. 
