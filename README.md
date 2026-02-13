# etl-banks-pipeline/

# 🏦 ETL Pipeline – Top 10 Banks by Market Capitalization

ETL pipeline that extracts the **top 10 banks by market capitalization** from Wikipedia, transforms market cap values into multiple currencies, and loads the data into CSV and SQLite.

---

## 📌 Overview
This project demonstrates an end-to-end ETL workflow, including data extraction, transformation, storage, and basic SQL analysis.

---

## 💼 Business Value
This pipeline produces a clean, structured dataset that can be consumed by BI dashboards or research teams to compare bank market capitalization across multiple currencies.

---

## 🔄 ETL Pipeline

1. **Extract**
   - Scrapes bank data from Wikipedia using BeautifulSoup
2. **Transform**
   - Converts market capitalization from USD to GBP, EUR, and INR
   - Rounds values for reporting consistency
3. **Load**
   - Saves results to CSV
   - Loads data into a SQLite database
4. **Query**
   - Enables SQL queries for analysis and reporting

---

## 🛠️ Tech Stack
- **Python**
- **Pandas / NumPy**
- **BeautifulSoup**
- **SQLite**
- **SQL**

---
## 🚀 How to Run

bash
pip install -r requirements.txt
python banks_project.py
---



## 📂 Project Structure
etl-banks-pipeline/
├── banks_project.py
├── exchange_rate.csv
├── requirements.txt
└── README.md

## 🚧 Future Improvements
- Add data validation (schema checks and null handling)
- Implement retry and timeout logic for HTTP requests
- Load data into PostgreSQL or a cloud data warehouse
- Automate execution with cron or Airflow

📫 Contact

Andrés
GitHub: https://github.com/webmas10

LinkedIn: www.linkedin.com/in/andres-corredor-4b9229169


