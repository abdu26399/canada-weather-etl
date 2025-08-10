# Canada Weather ETL

End to end ETL that collects weather data for Canadian cities using OpenWeather, transforms it in Python, loads it into Azure SQL, and refreshes on a schedule with GitHub Actions. Power BI connects to the SQL table for dashboards.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)

## Overview

This project pulls current, hourly, and daily forecasts for five Canadian cities. Data is retrieved from OpenWeather Geocoding and One Call 3.0 APIs, flattened into a clean tabular format, and written to Azure SQL. A scheduled GitHub Actions workflow keeps the table up to date. Power BI reads directly from Azure SQL to power a report.

## Features

- Geocoding and weather retrieval from OpenWeather
- Flattened current, hourly, and daily data in one table
- Idempotent load into Azure SQL using a staging table and MERGE
- Configuration via YAML with secrets kept out of Git
- Automated refresh on a schedule with GitHub Actions
- Power BI ready model for quick reporting

## Architecture

1. Extract  
   - Geocoding API to get lat and lon for each city  
   - One Call 3.0 API for current, hourly, and daily data  

2. Transform  
   - Flatten nested JSON  
   - Split daily temperature dictionary into columns  
   - Clean wind fields and timestamps  

3. Load  
   - Write to a staging table in Azure SQL  
   - MERGE into target table with a unique key on city, type, timestamp  

4. Schedule  
   - GitHub Actions runs the pipeline on a fixed cadence

5. Visualize  
   - Power BI connects to Azure SQL table `dbo.WeatherDataClean`

## Tech Stack

- Python, Pandas, Requests, PyYAML
- SQLAlchemy, pyodbc
- Azure SQL Database
- GitHub Actions
- Power BI Desktop and Service
