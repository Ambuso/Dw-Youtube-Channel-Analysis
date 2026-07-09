# 📊 YouTube Analytics Data Pipeline

An end-to-end data engineering project that automatically extracts YouTube channel analytics, transforms and enriches the data using Apache Spark, stores it in PostgreSQL, orchestrates the workflow with Apache Airflow, and visualizes insights through Grafana.

## 🚀 Overview

This project demonstrates a production-style ETL pipeline for collecting and analyzing YouTube channel performance.

Instead of relying on YouTube Studio's limited analytics, this solution provides a customizable analytics platform capable of tracking historical trends, engagement metrics, publishing patterns, and video performance.

The entire workflow is automated using Apache Airflow and can be scheduled to run daily.

---

## 🏗️ Architecture

```
                   +----------------------+
                   |  YouTube Data API v3 |
                   +----------+-----------+
                              |
                              |
                      Data Extraction
                              |
                              v
                     Raw JSON Files
                              |
                              |
                    Apache Spark ETL
          (Cleaning + Enrichment + Feature Engineering)
                              |
                              v
                     PostgreSQL Database
                              |
                              |
                     Apache Airflow DAG
               (Workflow Orchestration & Scheduling)
                              |
                              v
                     Grafana Dashboard
                              |
                              v
                Interactive Analytics & Insights
```

---

# 📌 Features

- Automated YouTube Data API extraction
- Apache Spark data transformation
- PostgreSQL data warehouse
- Apache Airflow orchestration
- Grafana dashboard visualization
- Daily scheduled ETL jobs
- Performance classification
- Publishing time analysis
- Engagement analytics
- Historical trend monitoring

---

# 🛠️ Technology Stack

| Component | Technology |
|------------|------------|
| Language | Python |
| ETL | Apache Spark |
| Workflow | Apache Airflow |
| Database | PostgreSQL |
| Visualization | Grafana |
| API | YouTube Data API v3 |
| Environment | Azure Virtual Machine |
| Configuration | python-dotenv |

---

# 📂 Project Structure

```
youtube-analytics-pipeline/
│
├── airflow/
│   └── youtube_data_pipeline.py
│
├── extract.py
├── transform.py
├── requirements.txt
├── .env.example
├── README.md
│
├── data/
│   └── youtube_raw.json
│
└── dashboards/
    └── grafana-dashboard.json
```

---

# ⚙️ Pipeline Workflow

## 1. Extract

The extraction script connects to the YouTube Data API and automatically retrieves:

- Video IDs
- Titles
- Publish dates
- Views
- Likes
- Comments

Output:

```
youtube_raw.json
```

---

## 2. Transform

Apache Spark performs the transformation by:

- Cleaning raw data
- Casting numeric fields
- Removing invalid records
- Creating analytical dimensions
- Enriching timestamps
- Categorizing video performance

Generated features include:

- Year
- Month
- Week
- Hour
- Day of week
- Performance class

---

## 3. Load

The transformed dataset is written into PostgreSQL.

Table:

```
dataengineering.youtube_videos_enriched
```

---

## 4. Orchestration

Apache Airflow schedules and manages the pipeline.

Workflow:

```
Extract
      ↓
Transform
      ↓
Load
```

The DAG runs automatically every day.

---

## 📊 Dashboard

Grafana connects directly to PostgreSQL to visualize:

- Top viewed videos
- Likes vs comments
- Publishing heatmaps
- Video performance
- Historical trends
- Daily engagement
- Channel growth

---

# 📈 Sample Analytics

The dashboard provides insights such as:

- Top 10 performing videos
- Most engaging content
- Best publishing days
- Best publishing hours
- View distribution
- Performance classification
- Historical growth trends

---

# 🔧 Environment Variables

Create a `.env` file.

```env
YOUTUBE_API_KEY=YOUR_API_KEY

YOUTUBE_CHANNEL_ID=CHANNEL_ID

OUTPUT_FILENAME=youtube_raw.json

POSTGRES_USER=postgres

POSTGRES_PASSWORD=password

POSTGRES_HOST=localhost

POSTGRES_PORT=5432

POSTGRES_DB=youtube

POSTGRES_SSLMODE=disable
```

---

# ▶️ Installation

## Clone repository

```bash
git clone https://github.com/yourusername/youtube-analytics-pipeline.git

cd youtube-analytics-pipeline
```

---

## Create virtual environment

```bash
python -m venv venv
```

Activate

Windows

```bash
venv\Scripts\activate
```

Linux

```bash
source venv/bin/activate
```

---

## Install dependencies

```bash
pip install -r requirements.txt
```

---

## Run extraction

```bash
python extract.py
```

---

## Run transformation

```bash
python transform.py
```

---

## Start Airflow

```bash
airflow scheduler

airflow webserver
```

---

## Open Grafana

```
http://localhost:3000
```

---

# 📊 Sample SQL Query

Retrieve the top 10 most viewed videos.

```sql
SELECT *
FROM (
    SELECT DISTINCT ON ("videoId")
        "videoId",
        "title",
        "viewCount",
        "publishedAt"
    FROM dataengineering.youtube_videos_enriched
    ORDER BY "videoId","publishedAt" DESC
) sub
ORDER BY "viewCount" DESC
LIMIT 10;
```

---

# 📌 Challenges Solved

✔ API rate limiting

✔ Pagination handling

✔ Automated scheduling

✔ Missing value handling

✔ PostgreSQL integration

✔ Spark transformations

✔ Dashboard automation

---

# 🚀 Future Improvements

- Sentiment analysis using YouTube comments
- Machine learning for publish time prediction
- Automatic email reports
- Cross-platform analytics (TikTok, Instagram, X)
- Channel benchmarking
- Real-time streaming using Kafka

---

# 📸 Dashboard Preview

Add screenshots of your Grafana dashboards here.

```
images/dashboard-overview.png
images/top-videos.png
images/publishing-heatmap.png
```

---

# 👨‍💻 Author

**Ambuso Dismas**

Data Engineer | Geospatial Data Specialist

- GitHub: https://github.com/Ambuso
- LinkedIn: https://linkedin.com/in/ambuso-dismas

---

# ⭐ If you found this project useful

Give the repository a ⭐ on GitHub to support the project.
