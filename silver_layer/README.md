# Silver Layer

This directory contains the dbt project that manages all transformations for both the silver and gold layers.

## Structure

- `transformations/`: Contains the dbt project
  - `models/silver/`: Contains the silver models
  - `models/gold/`: Contains the gold models
  - `dbt_project.yml`: The dbt project configuration file
  - `profiles/profiles.yml`: The dbt profiles configuration file
  - `models/sources.yml`: The sources configuration file

## Purpose

The silver layer represents the transformed and cleaned data layer in the Medallion Architecture. It contains data that has been cleaned, validated, and transformed into a more usable format.

## Note

In this project, all transformations (silver and gold) are managed within a single dbt project located in this directory. The gold_layer directory contains symbolic links to the gold models defined here, providing a consistent top-level directory structure while maintaining a single dbt project for all transformations. 