# E-Commerce Data Warehouse on PostgreSQL

This repository contains a complete implementation of an end-to-end PostgreSQL-based e-commerce data warehouse. The project demonstrates how OLTP-style normalized schemas compare to dimensional star-schema architectures in analytical workloads.

The repository includes:

- A normalized (no-star) OLTP-style schema  
- A star-schema dimensional warehouse  
- OLAP benchmark queries for both databases  
- SQL assets for warehouse creation, ingestion, and analytical evaluation  

---

## Project Overview

Analytical systems require schemas optimized for aggregation, grouping, trend analysis, and multi-dimensional reporting. While OLTP databases emphasize normalization and frequent updates, data warehouses emphasize denormalized structures, simplified joins, and optimized read performance.

This project includes:

1. Extracting an e-commerce dataset  
2. Transforming the data into fact and dimension structures  
3. Loading both a normalized and star-schema datawarehouse
4. Executing OLAP benchmark queries across both environments  
5. Analyzing performance differences between the architectures  



---

## Schema Designs

### 1. Normalized (No-Star) Schema

This schema models the system using a traditional OLTP-style design:

- Follows normalization rules to reduce redundancy  
- Suitable for high-frequency writes and updates  
- Prioritizes transactional integrity  
- Requires multiple joins for analytical queries  
- Used as the baseline for performance comparisons  

Although robust for operational systems, this schema is not optimized for analytics.

---

### 2. Star Schema Data Warehouse

This schema uses dimensional modeling and includes:

- A central fact table with measurable metrics (e.g., revenue, quantity)  
- Dimension tables for products, customers, time, and locations  
- Denormalized structures enabling efficient analytical processing  
- Reduced join complexity  
- Better compatibility with BI tools and OLAP engines  

Star schemas significantly improve aggregation and reporting performance.

---

## OLAP Query Benchmarks

Analytical workloads executed on both schemas include:

- Total revenue by month  
- Sales by region or city  
- Product-level revenue breakdowns  
- Customer segmentation analytics  
- Category-level rollups  
- Time-series revenue trends  

Key differences:

- The normalized schema requires multiple joins per query  
- The star schema simplifies queries and improves performance  
- Dimensional modeling aligns more closely with analytical access patterns  

---
## Insights & Findings
- Normalized schemas are ideal for transactional systems but inefficient for analytical workloads due to deep join chains.
- Star schemas dramatically improve read performance and simplify OLAP query design.
- Dimensional models create more intuitive, business-friendly data structures.
- Separation of OLTP and OLAP environments avoids contention and improves overall system scalability.
- The warehouse structure is better suited for reporting dashboards, data mining, and trend analytics.
