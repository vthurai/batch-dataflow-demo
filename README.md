# Dataflow Batch Demo

## Background

The purpose of this demo is to demonstrate the Dataflow’s batch capability. It references TTC delay data stored on Google Cloud Storage that has been reformatted and converted to CSV prior to Dataflow ingestion.  Dataflow will produce the necessary ETL processes on each data source, collect the data as one bundle, and create a BigQuery table. 

### TTC Delay Data

The city of Toronto has provided delay information of TTC transits on their open data wepbage. Please reference the links below to learn more about each data source:
* [Subway](https://www.toronto.ca/city-government/data-research-maps/open-data/open-data-catalogue/transportation/#917dd033-1fe5-4ba8-04ca-f683eec89761)
* [Street Car](https://www.toronto.ca/city-government/data-research-maps/open-data/open-data-catalogue/transportation/#e8f359f0-2f47-3058-bf64-6ec488de52da)
* [Bus](https://www.toronto.ca/city-government/data-research-maps/open-data/open-data-catalogue/transportation/#bb967f18-8d90-defc-2946-db3543648bd6)

## Google Cloud Services Utilized
* [Cloud Dataflow](https://cloud.google.com/dataflow/)
* [BigQuery](https://cloud.google.com/bigquery/)
* [Cloud Storage](https://cloud.google.com/storage/)
* [Cloud Identity & Access Management](https://cloud.google.com/iam/)

## Architecture

Dataflow looks at 3 different blob storage locations to read each transit delay data plus an additional blob that contains mapping needed for the Subway delay data.  After parsing the data into the correct [BigQuery Schema](src/main/resources/TransitDelaySchema.json) specified in the table, dataflow will create a new table (truncate data if existing) in BigQuery

![alt text](src/main/resources/images/batch-dataflow-demo.jpg)

## Results

TBD