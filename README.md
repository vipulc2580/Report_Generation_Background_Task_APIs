# Restaurants Report Generation(Background Tasks) APIs
*A modern FastAPI application for generating reports asynchronously using Celery, Redis, and PostgreSQL.*

![Carbon FootPrint Banner](https://github.com/vipulc2580/Report_Generation_Background_Task_APIs/blob/main/api_docs_screenshot.png)  

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Acknowledgements](#acknowledgements)

## Introduction

Restaurants Report Generation Background Task APIs is a backend service built with FastAPI that allows clients to request report generation tasks. These tasks—such as PDF or CSV generation—are offloaded to Celery workers for asynchronous processing. Results are stored and made available upon completion, ensuring a resilient and efficient architecture.

## Features

- **Asynchronous Report Processing**  
  Users submit report requests (such as data aggregations) and receive immediate acknowledgment while Celery handles processing in the background.

- **Report Status Tracking**  
  Clients can query endpoints to check the status of report generation tasks (e.g., pending, in-progress, completed, failed).

- **Result Retrieval**  
  Once ready, clients can download generated report files (e.g., PDFs, Excel, CSV).

- **Database Persistence**  
  PostgreSQL (initial database created as Restaurants) stores metadata about tasks, user requests, and

- **Caching & Messaging**  
 Redis serves as both the Celery broker and cache layer for quick status checks and inter-service communication.


## Technologies Used

- **Backend:**:Python, FastAPI
- **Database:**:PostgreSQL
- **Task Queue & Cache**:Celery, Redis (broker & cache)
- **Others:**:Pydantic models, Background processing with Celery, Redis caching

## Installation & Setup

1. **Clone the repository:**

   ```bash
   https://github.com/vipulc2580/Report_Generation_Background_Task_APIs
   cd Report_Generation_Background_Task_APIs
    ```

2. **Create and activate a virtual environment:**
    ```python 
      python -m venv env
      env\Scripts\activate  # On macOS/Linux use: source env/bin/activate
    ```

3. **Install dependencies:**
    ```python
       pip install -r requirements.txt
    ```    

4. **Configure PostgreSQL database:**
    - Create a database named Restaurant.
    - Update the database credentials in your .env as example_env

5. **Start Redis server (for Celery broker and cache):**
    ```bash
     redis-server
    ```
6. **Run database migrations (if applicable, e.g., using Alembic):**
    ``` bash
      alembic upgrade head
    ```
    
7. **Start Celery worker:**
   ```python
    celery -A src.utils.celery_tasks worker --pool=solo --loglevel=info
   ```

7. **Run the FastAPI server:**
    ```python
      fastapi dev .\src     
    ```

8. **Access the application:**
    - FastAPI docs: http://localhost:8000/api/v1/docs

# Usage
  ## User Flow:
  - Submit a report request via get to the endpoint /reports/trigger-report.
  - Receive report ID in response to track progress.
  - Poll status,report via a post request at endpoint, /reports/get-report
  - Download report when completed from /reports/get-report
  
# API Documentation
  - FastAPI automatically generates interactive docs available at:
    - http://localhost:8000/api/v1/docs
  - Report Generation Service Endpoints: /reports/trigger-report, /reports/get-report.
 
 # Acknowledgements
  - Thanks to FastAPI, Streamlit, Celery, Redis, and PostgreSQL communities for enabling modern web development.
  - Inspired by environmental sustainability initiatives and modern data-driven applications.
  - Special thanks to open-source contributors who maintain libraries used in this project.

