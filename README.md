# Data Engineering Coding Test

This repository contains a data engineering pipeline built with Luigi, Apache Spark, and FastAPI. The system processes film data, performs data quality checks using Great Expectations, and provides a REST API for querying and finding similar films.

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed on your system
- At least 4GB of available RAM (for Spark operations)

### Starting the Application

1. **Clone the repository** (if not already done):
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. **Start the application using Docker Compose**:
   ```bash
   docker-compose up --build
   ```

   This command will:
   - Build the Docker image
   - Start the Luigi scheduler
   - Run the data pipeline (import, genre split, and data validation)
   - Start the FastAPI application

3. **Wait for the pipeline to complete** - You'll see output indicating the Luigi pipeline progress and completion.

## 📊 Luigi UI

The Luigi UI provides a web interface to monitor and manage your data pipelines.

### Accessing Luigi UI
- **URL**: http://localhost:8082
- **Available after**: The Docker container starts and Luigi scheduler is running

### What you can do in Luigi UI:
- View pipeline task status and dependencies
- Monitor task execution progress
- Check task history and logs
- Trigger manual task runs
- Visualize the pipeline workflow

### Pipeline Tasks
The system runs three main Luigi tasks:
1. **Import Task** - Imports film data from CSV
2. **Genre Split Task** - Splits films by genre and creates separate datasets
3. **Data Check Task** - Performs data quality validation using Great Expectations

## 🔌 API Usage

The FastAPI application provides two main endpoints for querying film data.

### Base URL
- **API Base**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs (Swagger UI)

### Available Endpoints

#### 1. Filter Films (`GET /films`)
Query films with various filters:

```bash
# Basic usage
curl "http://localhost:8000/films"

# Filter by director
curl "http://localhost:8000/films?directors=Christopher%20Nolan"

# Filter by multiple criteria
curl "http://localhost:8000/films?genres=Action&release_year_min=2020&duration_min=120"

# Filter by cast members
curl "http://localhost:8000/films?cast=Tom%20Hanks&cast=Emma%20Watson"
```

**Query Parameters:**
- `directors` (list): Filter by director names
- `countries` (list): Filter by countries
- `genres` (list): Filter by genres
- `cast` (list): Filter by cast members
- `adult` (boolean): Filter by adult content
- `release_year_min` (int): Minimum release year
- `release_year_max` (int): Maximum release year
- `duration_min` (int): Minimum duration in minutes
- `duration_max` (int): Maximum duration in minutes

#### 2. Find Similar Films (`GET /similar_films/{film_id}`)
Find films similar to a specific film:

```bash
# Find similar films to a specific film ID
curl "http://localhost:8000/similar_films/tt0111161"

# With custom similarity threshold (default: 50.0)
curl "http://localhost:8000/similar_films/tt0111161?threshold=70.0"
```

**Parameters:**
- `film_id` (path): The ID of the film
- `threshold` (query, optional): Similarity threshold (0-100, default: 50.0)


## 🛠️ Development

### Project Structure
```
├── api/                    # FastAPI application
│   ├── main.py            # API endpoints
│   ├── data.py            # Data access layer
│   └── film_similarity.py # Similarity calculation
├── pipeline/              # Luigi pipeline tasks
│   └── tasks/
│       ├── task_import.py      # Data import task
│       ├── task_genre_split.py # Genre splitting task
│       └── task_ge_check.py    # Data validation task
├── resources/             # Input data files
│   ├── csv/
│   └── json/
├── output/               # Pipeline output directory
├── utils/                # Utility functions
└── docker-compose.yml    # Docker configuration
```

### Stopping the Application
```bash
# Stop the containers
docker-compose down

# Stop and remove volumes (clears data)
docker-compose down -v
```

### Production Readiness Improvements

When moving this project beyond a coding test, consider addressing the following areas:

#### Testing & Quality Assurance
- **Great Expectations Enhancement**: Implement more sophisticated data quality checks including:
  - Column value distributions and statistical tests
  - Custom expectation suites for business rules
- **API Testing**: Add automated API tests with proper authentication/authorization
- **Performance Testing**: Load testing for API endpoints and pipeline performance benchmarks
- **Container Orchestration**: Move from Docker Compose to Kubernetes or Docker Swarm for production
- **CI/CD Pipeline**: Implement automated testing, building, and deployment pipelines
- **Monitoring & Logging**: Add proper logging, metrics collection, and alerting (e.g., Prometheus, Grafana)
- **Database Integration**: Replace file-based storage with proper database systems (PostgreSQL, MongoDB)
- **Configuration Management**: Externalize configuration using environment variables or config management tools
- **Authentication & Authorization**: Implement proper user authentication and role-based access control
- **Spark Optimization**: Configure Spark for production workloads with proper resource allocation
- **Error Handling**: Robust error handling and retry mechanisms
- **Data Backup & Recovery**: Implement backup strategies and disaster recovery plans
- **Documentation**: Comprehensive technical documentation and API specifications

