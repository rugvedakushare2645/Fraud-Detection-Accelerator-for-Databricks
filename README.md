# Real-Time Fraud Detection Accelerator

This project introduces a fraud detection accelerator designed to identify fraudulent credit card transactions by analyzing various parameters. The system employs a multi-layered architecture, comprising Bronze, Silver, and Gold layers, to process and transform raw data into actionable insights.

## System Architecture

The accelerator's architecture is structured to ensure efficient data flow, cleaning, and sophisticated fraud detection.

### Bronze Layer

In the Bronze layer, raw data from various source tables is ingested and consolidated into a unified transaction table. This initial stage focuses on collecting all relevant data and performing basic transformations to prepare it for subsequent processing.

### Silver Layer

The Silver layer is dedicated to data cleaning and enrichment. This involves:
* **Null Value Removal**: Identifying and eliminating null values to ensure data quality.
* **Fraud Condition Flags**: Adding various flags based on predefined fraud conditions. Each flag serves as an individual feature, contributing to the overall determination of a transaction's legitimacy. This layer is crucial for feature engineering.

### Gold Layer

The Gold layer applies the core fraud detection logic. Here, a transaction is flagged as fraudulent if it exhibits more than three active fraud condition flags. The processed and labeled data from this layer is then utilized for creating comprehensive dashboards, providing visual insights into fraudulent activities.

## Data Schema

The following tables and their respective columns are used in this project:

### `devices.csv`
* `device_id`
* `device_type`
* `os`

### `cards.csv`
* `card_id`
* `card_type`
* `bank_id`

### `transactions.csv`
* `transaction_id`
* `card_id`
* `merchant_id`
* `website_id`
* `location_id`
* `device_id`
* `ip_id`
* `type_id`
* `amount`
* `currency`
* `timestamp`
* `is_fraud`

### `merchants.csv`
* `merchant_id`
* `merchant_name`
* `category`

### `fraud_labels.csv`
* `is_fraud`
* `label`

### `banks.csv`
* `bank_id`
* `bank_name`

### `transaction_types.csv`
* `type_id`
* `type_name`

### `ips.csv`
* `ip_id`
* `ip_address`
* `geo_location`

### `websites.csv`
* `website_id`
* `domain_name`
* `site_category`
* `site_reputation_score`
* `is_blacklisted`

### `locations.csv`
* `location_id`
* `country`
* `city`
* `region`
