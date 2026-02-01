# CryptoBot with Binance - Data Engineering Project

## 📖 Project Overview
This project is part of the DataScientest training (Analytics Engineer). The goal is to build a complete ETL/ELT pipeline that retrieves cryptocurrency data from the Binance API, models it, and stores it in a Data Warehouse.

We aim to analyze volatile financial markets based on Blockchain technology by collecting pricing data (e.g., BTC-USDT, BTC-ETH).

## 👥 Team Members
* **[Maren Handwerk]** - *maren-handwerk*
* **[Name Kollege 1]** - *Github Username*
* **[Name Kollege 2]** - *Github Username*

## 🛠️ Tech Stack & Tools
Based on the project requirements:
* **Language:** Python
* **Data Source:** Binance API
* **Database/Warehousing:** Snowflake (or PostgreSQL/BigQuery)
* **Orchestration/ETL:** Snaplogic / PySpark / Custom Python Scripts
* **Visualization:** Power BI / Tableau
* **Environment:** Virtual Environment (venv), VS Code

## 📅 Project Steps & Roadmap
1.  **Unstructured Data Exploration:**
    * Connect to Binance API.
    * Collect data samples (JSON).
    * Create a generic data retrieval function.
2.  **Data Modeling:**
    * Design a Star Schema (3NF/Denormalization).
    * Create UML diagrams.
3.  **ETL Pipeline Construction:**
    * **Extract:** Load data from API sources.
    * **Load:** Denormalize and load into Snowflake/SQL Database.
    * **Transform:** Merge and encode data.
4.  **Dashboarding:**
    * Create a final dashboard to visualize market trends.

## 🚀 Setup & Installation (For Developers)

### 1. Clone the repository
```bash
git clone https://github.com/maren-handwerk/CryptoBot_Binance_ETL.git
cd CryptoBot_Binance_ETL

# Create and activate Virtual Environment
# Mac/Linux
python -m venv .venv
source .venv/bin/activate

# Windows
python -m venv .venv
.\.venv\Scripts\activate

# Install Requirements
pip install -r requirements.txt