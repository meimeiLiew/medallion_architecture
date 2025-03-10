# Gold Layer

This directory contains symbolic links to the gold models defined in the dbt project located in the silver_layer directory.

## Structure

- `transformations/models/gold/`: Contains symbolic links to the gold models
- `transformations/dbt_project.yml`: Symbolic link to the dbt project file
- `transformations/profiles/profiles.yml`: Symbolic link to the dbt profiles file
- `transformations/models/sources.yml`: Symbolic link to the sources.yml file

## Purpose

The gold layer represents the business-ready analytics layer in the Medallion Architecture. It contains aggregated and denormalized data models that are optimized for business intelligence and reporting.

## Note

In this project, all transformations (silver and gold) are managed within a single dbt project located in the silver_layer directory. The symbolic links in this directory provide a consistent top-level directory structure while maintaining a single dbt project for all transformations. 