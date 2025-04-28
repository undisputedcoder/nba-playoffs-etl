# 2024 NBA Playoffs Statistics ETL pipeline

A basic ETL pipeline for extracting statistics from [basketball-reference](https://www.basketball-reference.com/) about the 2024 NBA playoffs and loading the data into a postgres database.


## Architecture
![Architecture diagram](./images/architecture-diagram.png)

## How it works
The Apache Airflow dag carries out the following tasks:
1. The data is extracted by scraping the [url](https://www.basketball-reference.com/playoffs/NBA_2024_per_game.html) containing the per game statistics about every player competing in the 2024 NBA post-season in a table and assigned to a pandas Dataframe

2. Data transformation is then applied to the Dataframe (like replacing `Nan` values with 0 and removing the Rk column) and saving the newly transformed data in a csv file
3. The data from the csv file is then inserted into the postgres database 

## Running the project
Run docker-compose.yaml to spin up the containers
```
docker-compose up -d
```

Go to **localhost:8080/home** and trigger the dag called `2024_nba_playoffs`

Once the dag has completed running, the Postgres database should be populated with the appropriate data