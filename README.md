# Employee Payroll Join (Hadoop MapReduce)

## 📌 Overview

This project performs a Reduce-Side Join between employee data and payroll data using Hadoop MapReduce.

## 🎯 Objective

* Join employee and payroll datasets
* Compute total monthly salary
* Track maximum salary per employee

## 🏗️ Architecture

* Two Mappers:

  * EmployeeMapper
  * PayrollMapper
* One Reducer:

  * PayrollReducer
* Join happens in Reducer

## ⚙️ Tech Stack

* Java
* Hadoop MapReduce
* Maven

## 📂 Project Structure

* mapper/
* reducer/
* driver/
* utils/

## 🧪 Sample Input

### Employees

EMP01,Nour,Hassan,Engineering

### Payroll

PR001,EMP01,Jan,8000,500

## 📤 Sample Output

EMP01 Nour Hassan,Engineering,Jan,8500,8700

## ⚠️ Edge Cases

* Unknown employees handled
* Invalid salary records skipped

## 🚀 How to Run

```bash
mvn clean package
hadoop jar target/payroll.jar com.payroll.driver.PayrollDriver input output
```

## 📈 Notes

* Reduce-Side Join used
* Combiner not applicable
