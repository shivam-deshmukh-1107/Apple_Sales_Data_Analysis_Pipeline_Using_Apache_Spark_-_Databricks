#  Apple Sales Data Analysis Pipeline using Apache Spark and Databricks

## 📌 Project Overview
This project involves building a complete end-to-end **ETL (Extract, Transform, Load)** data pipeline using **Apache Spark (PySpark)** on the **Databricks** platform to analyze Apple product sales data. The pipeline derives actionable insights like identifying customers who bought AirPods after an iPhone, customers with exclusive purchases, average delays between purchases, and top-selling products by category.

![Apple_Sales_Pipeline](https://github.com/user-attachments/assets/b07df107-d4e7-41ea-82c6-0427c6df5026)

---

## 🎯 Objectives
- Build a robust and scalable big data engineering pipeline.
- Use Apache Spark for distributed data processing.
- Leverage Databricks Lakehouse for reliable storage with ACID guarantees.
- Implement the Factory Design Pattern for modular code.
- Deliver business insights through meaningful data transformations.

---

## 🧰 Technologies Used
- **Apache Spark (PySpark)**
- **Databricks Platform**
- **Delta Lake**
- **File Formats**: CSV, Parquet, Delta Tables
- **Python Factory Design Pattern**

---

## ⚙️ Implementation Methodology

### 🏗️ Environment Setup
- Created a Databricks cluster and enabled DBFS.
- Uploaded datasets (CSV files) into DBFS.
- Created Delta tables.
- Initialized SparkSession using PySpark.

### 🔄 ETL Pipelines

#### 📍 Pipeline 1: Customers Who Bought AirPods After iPhone
- Applied `lead()` window function on transactions to identify sequential purchases.
- Filtered to find users who bought AirPods after buying an iPhone.
- Broadcast join used for optimization.
- Loaded results into DBFS and Delta tables.

#### 📍 Pipeline 2: Customers Who Bought Both AirPods and iPhone
- Aggregated purchases using `collect_set(product_name)`.
- Filtered customers with only “AirPods” and “iPhone” in their set.
- Partitioned final data by location and stored in Delta tables.

#### 📍 Pipeline 3: Products Bought After Initial Purchase
- Identified first purchase date per customer using `min(transaction_date)`.
- Filtered and aggregated products bought after the first transaction.

#### 📍 Pipeline 4: Average Delay Between iPhone & AirPods Purchase
- Identified timestamps of iPhone and AirPods purchases.
- Calculated average time delay between purchases for each customer.

#### 📍 Pipeline 5: Top 3 Products by Revenue in Each Category
- Joined transactions with product data to compute total revenue.
- Applied `dense_rank()` on revenue grouped by category.
- Selected top 3 products per category.

### 🔁 Workflow Management
- Designed modular components: `Extractor`, `Transformer`, `Loader`, and `Utils`.
- Implemented a `WorkflowRunner` to execute pipelines efficiently.
- Used Factory Pattern to support modularity and maintainability.

---

## 📊 Results and Outcomes
- Derived high-impact business insights from transaction data.
- Achieved fast performance using broadcast joins, partitioning, and predicate pushdown.
- Built a reusable, maintainable codebase ready for production-scale workloads.

---

## ✅ Conclusion
This project showcases the power of Apache Spark and Databricks for building scalable and efficient big data pipelines. It demonstrates advanced engineering practices using Factory Pattern and modular design for clean, production-grade ETL systems.

---

## 🚀 Future Additions
- Enable real-time data processing and streaming pipelines.
- Integrate machine learning models for behavior prediction and personalization.
- Use Adaptive Query Execution (AQE) and additional performance tuning strategies.

