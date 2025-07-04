# Databricks notebook source
try:
    _day = dbutils.jobs.taskValues.get("Job_by_day", "input_day")
    print(_day)
except ValueError as e:
    print(f"Error: {e}")