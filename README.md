# Weather Data Pipeline 

This project is a data engineering pipeline, orchestrated with **Apache Airflow**, that automates the collection of historical weather data for various cities and stores it efficiently for analysis.

The pipeline is built with a simple Extract, Transform, Load (ETL) workflow, fetching data from an external API and storing it in a partitioned Parquet format optimized for analytics.

### Features

* **Data Orchestration:** Uses Apache Airflow to schedule and manage the pipeline's workflow.
* **Data Collection:** Extracts historical weather data from an API for a pre-defined set of cities.
* **Efficient Storage:** Stores the data in a partitioned Parquet format, ideal for storage efficiency and query optimization.
* **Secure Configuration:** Uses environment variables to manage API keys, ensuring project security.

---

### Setup and Installation

To run this project locally, follow the steps below.

1.  **Clone the Repository**

    ```bash
    git clone [https://github.com/your-username/your-repo.git](https://github.com/your-username/your-repo.git)
    cd your-repo
    ```

2.  **Set Up the Virtual Environment**

    ```bash
    python3 -m venv my-env
    source my-env/bin/activate
    ```

3.  **Install Dependencies**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables**

    Create a `.env` file in the root of your project and add your Visual Crossing API key.

    ```
    VC_API_KEY=your-api-key-here
    ```

5.  **Initialize the Airflow Database**

    ```bash
    # Set your project folder as the Airflow "home"
    export AIRFLOW_HOME=.
    # Initialize the database
    airflow db init
    ```

6.  **Run the Pipeline**

    Open two separate terminal windows in the root of your project. In one, run the Airflow web server, and in the other, run the scheduler.

    ```bash
    # Terminal 1: Web Server
    airflow webserver --port 8080

    # Terminal 2: Scheduler
    airflow scheduler
    ```

7.  **Access the User Interface**

    After both commands are running, access the Airflow UI at `http://localhost:8080` to see your DAG and trigger a run.

---

### Usage

The `fetch_weather_data` DAG is defined to run daily, but you can trigger it manually from the Airflow UI.

1.  Open your browser and go to `http://localhost:8080`.
2.  Unpause the DAG.
3.  Click the play button to trigger a manual run.
4.  The output files will be saved in `data/weather_partitioned`.

---

### Project Structure