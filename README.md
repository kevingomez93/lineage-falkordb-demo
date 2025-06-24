# FalkorDB Data Lineage Proof-of-Concept for Despegar Data

This project demonstrates a proof-of-concept (POC) for building a data lineage graph using [FalkorDB](https://falkordb.com/). It models a simplified version of the data ecosystem at a Generic Data team, including models, tables, columns, jobs, KPIs, and dashboards, along with their relationships and ownership.

## What's Inside?

-   `poc.py`: The main Python script that connects to FalkorDB, cleans any existing data, builds the data lineage graph, and runs several example queries.
-   `requirements.txt`: The Python dependencies needed to run the script (just the `falkordb` client).
-   `Dockerfile`: Defines a container image for the Python script to run in an isolated environment.
-   `docker-compose.yml`: An orchestration file to easily launch the entire environment, including the FalkorDB database and the Python script that populates it.
-   `data/`: A directory that will be created to persist the FalkorDB graph data on your local machine.

## Prerequisites

-   [Docker](https://www.docker.com/products/docker-desktop/)
-   [Docker Compose](https://docs.docker.com/compose/install/) (usually included with Docker Desktop)

## How to Run

1.  **Clone the repository:**
    ```sh
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```

2.  **Start the environment:**
    Run the following command in your terminal:
    ```sh
    docker-compose up --build
    ```
    This command will:
    -   Build a Docker image for the Python script.
    -   Start a FalkorDB container.
    -   Wait for the database to be ready.
    -   Run the Python script, which connects to the database and populates it with the sample data lineage graph.
    -   The script will print the results of several example queries to your terminal.

3.  **Explore the Graph:**
    Once the services are running, you can explore the graph visually using the built-in web interface.
    -   Open your browser and navigate to **[http://localhost:3000](http://localhost:3000)**.
    -   To see the entire graph, run the following query in the UI:
        ```cypher
        MATCH p=(n)-[r]->(m) RETURN p
        ```

## How to Stop

To stop the services and shut down the environment, press `Ctrl+C` in the terminal where `docker-compose` is running, or run the following command from the project directory in another terminal:
```sh
docker-compose down
``` 
