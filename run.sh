#!/bin/bash

hdfs dfs -rm -r /output

hadoop jar target/employee-payroll.jar \
com.payroll.driver.PayrollDriver \
/input /output
