# 🚀 Employee Payroll Analytics Big Data Project

## 📌 Project Title

**Employee Payroll Analytics using Hadoop MapReduce (Reduce-Side Join)**

---

## 📝 Project Description

This project is a **Big Data processing pipeline** designed to analyze large-scale employee payroll records using **Hadoop MapReduce**.

The system performs a **Reduce-Side Join** between employee master data and payroll transaction datasets to generate enriched payroll analytics for millions of records efficiently.

### 🎯 Core Objectives

* Process large employee and payroll datasets in a distributed environment
* Perform scalable joins between multiple datasets
* Calculate employee salary insights such as:

  * Monthly total compensation
  * Maximum salary earned
  * Department-level organization
* Demonstrate real-world Big Data engineering practices
* Build a production-style Hadoop analytics workflow

---

# 🛠️ Tech Stack

| Technology                | Purpose                                  |
| ------------------------- | ---------------------------------------- |
| ☕ Java                    | Core application development             |
| 🐘 Hadoop MapReduce       | Distributed data processing              |
| 🗄️ HDFS                  | Distributed storage layer                |
| ⚙️ YARN                   | Cluster resource management              |
| 📦 Maven                  | Build automation & dependency management |
| 🐍 Python                 | Synthetic dataset generation             |
| 🐙 Git & GitHub           | Version control & portfolio hosting      |
| 💻 Cloudera QuickStart VM | Hadoop development environment           |
| 📄 CSV                    | Input/output data format                 |

---

# 🏗️ Architecture (Data Pipeline)

```text
Employee Dataset CSV ──► EmployeeMapper ──┐
                                          │
                                          ├──► Shuffle & Sort ──► PayrollReducer ──► Final Analytics Output
                                          │
Payroll Dataset CSV ───► PayrollMapper ──┘
```

## 🔄 Pipeline Stages

### 1️⃣ Data Ingestion

* Employee records loaded into HDFS
* Payroll transaction records loaded into HDFS

### 2️⃣ Mapping Phase

* EmployeeMapper emits employee metadata
* PayrollMapper emits payroll transaction data

### 3️⃣ Shuffle & Sort

* Hadoop groups records by `employee_id`

### 4️⃣ Reduce-Side Join

* Merges employee details with payroll history
* Computes salary analytics

### 5️⃣ Output Generation

* Produces enriched payroll reports for downstream analysis

---

# ✨ Key Features

## ✔ Distributed Payroll Processing

Handles millions of records efficiently using Hadoop.

## ✔ Reduce-Side Join Implementation

Joins two large datasets at scale.

## ✔ Salary Analytics

* Monthly salary calculation
* Bonus integration
* Maximum salary detection

## ✔ Data Validation & Fault Tolerance

* Malformed record detection
* Missing employee handling
* Counter-based monitoring

## ✔ Performance Metrics

Uses Hadoop Counters for:

* Records processed
* Missing records
* Invalid rows
* Reducer statistics

## ✔ Production-Oriented Structure

* Maven project setup
* Executable JAR
* Shell automation
* GitHub-ready documentation

---

# 📂 Project Structure

```bash
Employee-payroll-mapreduce/
│
├── src/                     # Java source code
├── Jar/                     # Compiled JAR file
├── Sample_data/             # Sample datasets
├── Screenshots/             # Execution proof & results
├── pom.xml                  # Maven configuration
├── run.sh                   # Automated execution script
└── README.md                # Project documentation
```

---

# ⚙️ Setup & Installation

## 🔧 Prerequisites

* Java 8+
* Hadoop (Cloudera / Apache)
* Maven
* HDFS configured
* Linux or Cloudera VM

---

## 📥 Clone Repository

```bash
git clone https://github.com/mohamed-code342/Employee-payroll-mapreduce
cd Employee-payroll-mapreduce
```

---

### Output:

```bash
target/employeepayroll.jar
```

---

## 📤 Upload Data to HDFS

```bash
hdfs dfs -mkdir -p /employeepayroll/real
hdfs dfs -put employees_real.csv /employeepayroll/real/
hdfs dfs -put payroll_real.csv /employeepayroll/real/
```

---

# ▶️ Usage

##  Direct Execution

```bash
hadoop jar employeepayroll.jar employeepayroll.PayrollDriver \
-Dyarn.app.mapreduce.am.staging-dir=/user/cloudera/tmp \
-Dmapreduce.job.working.dir=/user/cloudera/tmp \
/employeepayroll/real/employees_real.csv \
/employeepayroll/real/payroll_real.csv \
/employeepayroll/output_real
```


## 📄 View Results

```bash
hdfs dfs -cat /employeepayroll/output_real/part-r-00000 | head
```

---

# 📊 Example Output

```text
EMP000001 Hana Mahmoud Sales Jan 33980 56558
EMP000001 Hana Mahmoud Sales Feb 26087 56558
EMP000001 Hana Mahmoud Sales Mar 35226 56558
```

| Field       | Description                |
| ----------- | -------------------------- |
| Employee ID | Unique employee identifier |
| Full Name   | Employee full name         |
| Department  | Organizational department  |
| Month       | Payroll month              |
| Total Pay   | Salary + bonus             |
| Max Pay     | Highest salary recorded    |

---

# 📈 Performance Highlights

## Real Dataset Scale

* 👨‍💼 1,000,000 employee records
* 💰 12,000,000 payroll records
* 📦 13,000,000 total processed records

## Hadoop Metrics

* 5 Mapper tasks
* 1 Reducer task
* 12M+ final output records
* Full distributed join success

---

# 🔮 Future Enhancements

## 🚀 Planned Improvements

* Apache Spark migration for faster in-memory processing
* Hive integration for SQL-like payroll querying
* Kafka streaming for real-time payroll ingestion
* Interactive dashboard using Dash or Power BI
* Department salary aggregation reports
* Machine Learning salary trend prediction
* Cloud deployment on AWS EMR / Azure HDInsight

---

# 📚 Learning Outcomes

This project demonstrates:

* Big Data architecture design
* Hadoop ecosystem integration
* Distributed joins
* Performance optimization
* Data engineering workflows
* Scalable analytics systems

---

# ⭐ Final Note

This repository represents a practical implementation of a **Big Data payroll analytics system** and serves as a strong showcase for:

* Data Engineering
* Hadoop Development
* Distributed Systems
* Academic Big Data Projects
* Technical Portfolio Building
