# Employee Payroll MapReduce (Hadoop Reduce-Side Join)

## 📌 Project Overview

This project implements a **distributed Employee Payroll Analytics system** using **Hadoop MapReduce** with a **Reduce-Side Join** strategy.

The system processes large-scale employee and payroll datasets to combine employee profile information with monthly payroll transactions, producing enriched analytics output for each employee.

### 🎯 Main Goals

* Process **millions of employee and payroll records efficiently**
* Perform a **Reduce-Side Join** between:

  * Employee dataset
  * Payroll dataset
* Calculate:

  * Employee full name
  * Department
  * Monthly total pay
  * Maximum salary earned by each employee
* Demonstrate scalable Big Data processing using Hadoop ecosystem tools

---

# 🏗️ Technologies Used

| Technology             | Purpose                         |
| ---------------------- | ------------------------------- |
| Java                   | Core implementation             |
| Hadoop MapReduce       | Distributed data processing     |
| HDFS                   | Distributed storage             |
| YARN                   | Resource management             |
| Maven                  | Dependency management & build   |
| Cloudera QuickStart VM | Development environment         |
| GitHub                 | Version control & documentation |

---

# 📂 Project Structure

```bash
Employee-payroll-mapreduce/
│
├── src/
│   └── employeepayroll/
│       ├── EmployeeMapper.java
│       ├── PayrollMapper.java
│       ├── PayrollReducer.java
│       └── PayrollDriver.java
│
├── Sample_data/
│   ├── employees_sample.csv
│   └── payroll_sample.csv
│
├── Screenshots/
│   ├── reduce_side_join_job_success1.png
│   ├── reduce_side_join_job_success2.png
|   ├── reduce_side_join_job_success3.png
│   ├── input1.png
│   ├── input2.png
│   └── output.png
│
├── Jar/
│   └── employeepayroll.jar
│
└── README.md
```

---

# 📊 Dataset Description

## 👨‍💼 Employee Dataset

Contains employee personal and organizational information.

### Format:

```csv
employee_id,first_name,last_name,department
```

### Example:

```csv
EMP000001,Hana,Mahmoud,Sales
EMP000002,Mona,Khaled,IT
```

---

## 💰 Payroll Dataset

Contains monthly payroll transactions for employees.

### Format:

```csv
employee_id,month,base_salary,bonus
```

### Example:

```csv
EMP000001,Jan,32000,1800
EMP000001,Feb,31000,2200
```

---

# 🔄 System Workflow

## Step 1: EmployeeMapper

Processes employee records and emits:

```text
key = employee_id
value = emp~firstName,lastName,department
```

---

## Step 2: PayrollMapper

Processes payroll records and emits:

```text
key = employee_id
value = pay~month,baseSalary,bonus
```

---

## Step 3: PayrollReducer

Reducer:

* Joins employee + payroll data
* Calculates total pay
* Finds max pay for each employee
* Outputs enriched payroll analytics

### Final Output:

```text
employee_id fullName department month totalPay maxPay
```

### Example:

```text
EMP000001 Hana Mahmoud Sales Jan 33980 56558
```

---

# 🧠 Reduce-Side Join Architecture

```text
Employee Data ----> EmployeeMapper ---\
                                      ---> Shuffle/Sort ---> PayrollReducer ---> Final Output
Payroll Data -----> PayrollMapper ----/
```

---

# ⚙️ Build Instructions

## Using Maven:

```bash
mvn clean package
```

### Output:

```bash
target/employeepayroll.jar
```

---

# 🚀 Running the Project

## Direct Hadoop Command

```bash
hadoop jar employeepayroll.jar employeepayroll.PayrollDriver \
-Dyarn.app.mapreduce.am.staging-dir=/user/cloudera/tmp \
-Dmapreduce.job.working.dir=/user/cloudera/tmp \
/employeepayroll/real/employees_real.csv \
/employeepayroll/real/payroll_real.csv \
/employeepayroll/output_real
```


---

# 📈 Performance Scale

## Real Dataset Used:

* **1,000,000 Employees**
* **12,000,000 Payroll Records**
* **13,000,000 Total Input Records**

### Hadoop Job Metrics:

* Reduce Input Groups: 1,000,000
* Reduce Output Records: 12,000,000
* Successful Distributed Join
* Large-scale processing with fault tolerance

---

# 📸 Screenshots

## 🟢 Job Submission

Shows Hadoop job initialization and cluster submission.

## 📊 MapReduce Progress

Displays mapper and reducer progress percentages.

## ✅ Job Completion

Successful execution confirmation.

## 📉 Hadoop Counters

Includes:

* Lines processed
* Records emitted
* Reducer statistics
* Memory usage

## 📤 Output Sample

Displays joined payroll analytics results.

---

# 🔍 Key Features

## ✔ Robust Data Validation

* Blank line skipping
* Malformed record detection
* Numeric validation
* Negative salary rejection

## ✔ Hadoop Counters

Tracks:

* Input quality
* Output volume
* Missing records
* Join performance

## ✔ Optimized Memory Usage

* Reusable `Text` objects
* StringBuilder optimization
* Efficient reducer buffering

## ✔ Production-Style Engineering

* Clean code structure
* Maven support
* Scripted execution
* GitHub documentation

---

# 📌 Example Output

```text
EMP000001 Hana Mahmoud Sales Jan 33980 56558
EMP000001 Hana Mahmoud Sales Feb 26087 56558
EMP000001 Hana Mahmoud Sales Mar 35226 56558
```

### Field Explanation:

| Field        | Description       |
| ------------ | ----------------- |
| EMP000001    | Employee ID       |
| Hana Mahmoud | Full Name         |
| Sales        | Department        |
| Jan          | Payroll Month     |
| 33980        | Total Monthly Pay |
| 56558        | Max Employee Pay  |

---

# 🧪 Sample Data Included

To keep repository lightweight, only sample datasets are uploaded.

### Full dataset generation is supported using Python generators.

---

# 📚 Learning Outcomes

This project demonstrates:

* Big Data engineering
* Hadoop MapReduce architecture
* Reduce-Side Join implementation
* Distributed system optimization
* Large dataset management
* Software engineering best practices


---

# 🌟 Future Improvements

* Apache Spark migration
* Hive integration
* Data visualization dashboard
* Salary trend analysis
* Departmental payroll aggregation

---

# 📜 License

This project is developed for educational and portfolio purposes.

---

# ⭐ Final Note

This project showcases practical implementation of **Big Data processing pipelines** and **distributed payroll analytics** using Hadoop MapReduce, making it suitable for:

* Academic projects
* Big Data portfolios
* Data engineering showcases
* Distributed systems demonstrations
