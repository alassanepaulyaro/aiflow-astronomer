# Airflow with Astronomer - Learning Project

## Overview

This project is an Apache Airflow learning environment built using Astronomer's Astro CLI. It demonstrates various Airflow concepts including DAG creation, TaskFlow API, XCom for data passing, and dynamic task mapping. The project includes multiple example DAGs that showcase different workflow patterns and Airflow features.

## Table of Contents

- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [DAG Descriptions](#dag-descriptions)
- [Project Files](#project-files)
- [Development Workflow](#development-workflow)
- [Accessing Airflow](#accessing-airflow)
- [Useful Commands](#useful-commands)
- [Learning Resources](#learning-resources)

## Project Structure

```
aiflow-astronomer/
├── dags/                          # Directory containing all Airflow DAGs
│   ├── exampledag.py             # Astronaut ETL example with dynamic task mapping
│   ├── maths_operation.py        # Mathematical operations pipeline using XCom
│   ├── mlpipline.py              # Basic ML pipeline workflow
│   ├── taskflowapi.py            # TaskFlow API demonstration
│   └── .airflowignore            # Files to ignore in DAG parsing
├── tests/                         # Test directory for DAG validation
│   └── dags/                     # DAG test files
├── .astro/                        # Astronomer configuration directory
├── Dockerfile                     # Docker image configuration (Astro Runtime 12.6.0)
├── requirements.txt               # Python dependencies
├── packages.txt                   # OS-level package dependencies (empty)
├── .dockerignore                  # Docker build ignore patterns
├── .gitignore                     # Git ignore patterns
└── README.md                      # This file
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) (running)
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed
- Minimum 4GB RAM allocated to Docker
- Ports 8080 (Airflow Webserver) and 5432 (PostgreSQL) available

## Installation & Setup

### 1. Clone or Navigate to the Project

```bash
cd c:\Users\ELCLEO\Documents\CICD\aiflow-astronomer
```

### 2. Start the Airflow Environment

```bash
astro dev start
```

This command will spin up 4 Docker containers:
- **Postgres**: Airflow's metadata database
- **Webserver**: Airflow UI (accessible at http://localhost:8080)
- **Scheduler**: Monitors and triggers tasks
- **Triggerer**: Handles deferred tasks

### 3. Verify Containers are Running

```bash
docker ps
```

You should see 4 containers related to this project.

### 4. Access the Airflow UI

- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 5. Access PostgreSQL Database (if needed)

- Host: `localhost`
- Port: `5432`
- Database: `postgres`

## DAG Descriptions

### 1. Example Astronauts DAG ([exampledag.py](dags/exampledag.py))

**DAG ID**: `example_astronauts`

**Description**: An ETL pipeline that queries the Open Notify API to retrieve the list of astronauts currently in space and prints information about each astronaut.

**Key Features**:
- Uses TaskFlow API with `@task` decorators
- Implements dynamic task mapping to create tasks for each astronaut
- Demonstrates XCom data passing
- Uses Dataset outlets for downstream DAG triggering
- Error handling with fallback data
- Retry logic (3 retries)

**Schedule**: Daily (`@daily`)

**Tasks**:
1. `get_astronauts`: Fetches astronaut data from API
2. `print_astronaut_craft`: Dynamically mapped task that prints each astronaut's info

### 2. Math Sequence DAG ([maths_operation.py](dags/maths_operation.py))

**DAG ID**: `math_sequence_dag`

**Description**: A mathematical pipeline that performs sequential operations on a number, demonstrating XCom usage for passing data between tasks.

**Workflow**:
1. Start with number 10
2. Add 5 → Result: 15
3. Multiply by 2 → Result: 30
4. Subtract 3 → Result: 27
5. Square the result → Result: 729

**Key Features**:
- Uses PythonOperator for task creation
- Demonstrates XCom push/pull operations
- Sequential task dependencies
- Context management with `provide_context`

**Schedule**: Once (`@once`)

### 3. ML Pipeline DAG ([mlpipline.py](dags/mlpipline.py))

**DAG ID**: `ml_pipeline`

**Description**: A basic machine learning pipeline template demonstrating a typical ML workflow structure.

**Tasks**:
1. `preprocess_task`: Data preprocessing step
2. `train_task`: Model training step
3. `evaluate_task`: Model evaluation step

**Key Features**:
- Simple linear pipeline structure
- Template for ML workflows
- Uses PythonOperator

**Schedule**: Weekly (`@weekly`)

### 4. TaskFlow API Math DAG ([taskflowapi.py](dags/taskflowapi.py))

**DAG ID**: `math_sequence_dag_with_taskflow`

**Description**: A refactored version of the math sequence DAG using Airflow's modern TaskFlow API, demonstrating cleaner syntax and automatic dependency management.

**Key Features**:
- Uses `@task` decorators for cleaner code
- Automatic XCom handling (no manual push/pull)
- Implicit dependency inference
- Return values automatically passed between tasks
- More Pythonic and intuitive

**Schedule**: Once (`@once`)

**Comparison with Traditional Approach**:
- No need for `PythonOperator`
- No manual `provide_context`
- No explicit XCom operations
- Dependencies defined through function calls

## Project Files

### Core Configuration Files

#### [Dockerfile](Dockerfile)
- Base image: `quay.io/astronomer/astro-runtime:12.6.0`
- Can be customized to install additional dependencies or execute runtime commands

#### [requirements.txt](requirements.txt)
- Python package dependencies
- Currently empty (using Astro Runtime pre-installed packages)
- Add Python packages here as needed (e.g., `pandas`, `scikit-learn`, `requests`)

#### [packages.txt](packages.txt)
- OS-level package dependencies
- Currently empty
- Add system packages here (e.g., `gcc`, `build-essential`)

#### [.gitignore](.gitignore)
- Excludes development artifacts from version control
- Ignores: `.env`, `airflow_settings.yaml`, `__pycache__/`, `.venv`, etc.

## Development Workflow

### Adding a New DAG

1. Create a new Python file in the [dags/](dags/) directory
2. Define your DAG using either:
   - **TaskFlow API** (recommended): Use `@dag` and `@task` decorators
   - **Traditional approach**: Use `DAG()` context manager with operators

Example TaskFlow DAG:
```python
from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"]
)
def my_new_dag():
    @task
    def my_task():
        print("Hello from my task!")
        return "Task output"

    my_task()

my_new_dag()
```

3. Save the file
4. The DAG will automatically appear in the Airflow UI within 30 seconds

### Adding Python Dependencies

1. Add the package name to [requirements.txt](requirements.txt):
   ```
   pandas==2.0.0
   requests==2.31.0
   ```

2. Restart the Airflow environment:
   ```bash
   astro dev restart
   ```

### Testing DAGs

1. Use the Airflow UI to manually trigger DAGs
2. Monitor task logs in the UI
3. Add validation tests in the [tests/dags/](tests/dags/) directory

## Accessing Airflow

### Airflow Web UI
- **URL**: http://localhost:8080
- **Username**: `admin`
- **Password**: `admin`

### Features Available in UI:
- View all DAGs and their status
- Trigger manual DAG runs
- View task logs and execution history
- Monitor task duration and performance
- Manage connections and variables
- View XCom data
- Access Gantt charts and task dependencies

## Useful Commands

### Astronomer CLI Commands

```bash
# Initialize a new Airflow project
astro dev init

# Start Airflow locally
astro dev start

# Stop Airflow
astro dev stop

# Restart Airflow (use after modifying Dockerfile or requirements.txt)
astro dev restart

# View running containers
astro dev ps

# View scheduler logs
astro dev logs -s

# View webserver logs
astro dev logs -w

# Run Airflow CLI commands
astro dev run <airflow-command>

# Example: List all DAGs
astro dev run dags list

# Example: Test a specific task
astro dev run tasks test <dag_id> <task_id> <execution_date>

# Access Airflow scheduler bash
astro dev bash -s

# Kill and remove all containers (clean slate)
astro dev kill
```

### Docker Commands

```bash
# View all running containers
docker ps

# View container logs
docker logs <container_id>

# Stop all project containers
docker-compose down

# Remove all stopped containers and volumes
docker system prune -a --volumes
```

## Learning Resources

### Official Documentation
- [Astronomer Documentation](https://www.astronomer.io/docs/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)

### Tutorials
- [Getting Started with Airflow (Astronomer)](https://www.astronomer.io/docs/learn/get-started-with-airflow)
- [Write Your First DAG](https://www.astronomer.io/docs/learn/get-started-with-airflow)
- [Airflow Concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts/index.html)

### Key Concepts Demonstrated in This Project
1. **DAG Creation**: Using `@dag` decorator and context managers
2. **Task Definition**: Traditional operators vs TaskFlow API
3. **XCom**: Passing data between tasks (explicit and implicit)
4. **Dynamic Task Mapping**: Creating tasks dynamically based on data
5. **Scheduling**: Different schedule intervals (`@daily`, `@weekly`, `@once`)
6. **Error Handling**: Retries and fallback logic
7. **Dataset Awareness**: Using Dataset outlets for triggering

## Troubleshooting

### Port Already Allocated
If port 8080 or 5432 is already in use:
- Stop conflicting services, or
- Change ports in Astronomer configuration

### DAGs Not Appearing
- Check DAG file for syntax errors
- View scheduler logs: `astro dev logs -s`
- Ensure DAG is not paused in the UI

### Docker Issues
- Ensure Docker Desktop is running
- Allocate at least 4GB RAM to Docker
- Try: `astro dev kill` then `astro dev start`

## Project Information

- **Astro Runtime Version**: 12.6.0
- **Python Version**: Included in Astro Runtime
- **Airflow Version**: Included in Astro Runtime

## License

This project is for educational purposes.
