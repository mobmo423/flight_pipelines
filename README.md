![Excalidraw Image](images/Untitled-2023-02-13-0758.png)

# flight_pipelines

--- COPY FROM INSTRUCTIONS

# Project plan 

## Objective  
We will compare the flights statistics of three representative airports (Washington DC, Krakow, Tenerife) for some current dates. Comepetition between airports. 

## Consumers 
A data analyst will compare the flight statistics by creating a dashboard for executive management.

## Questions 
Does the percentage of delayed flights (together for arrivals and departures) differ between our three airports? Which airport is the most reliable?

## Source datasets
The flight statistics are sourced from an live flight tracking API on a daily basis (every 24h).

## Solution architecture
How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution? 

EXTRACT
- one table per request (colums? ids of aiport? format?)
- put it in format
- log data?

LOAD
- to database
- incremental (appending data)
- in addition: we save a parquet file named by the date of the API request
- log data? 

TRANSFORM
- source data from data base
- renaming 
- bronze/silver/gold
- group-by airport
- avr number of flights delayed for arrivals and departures?

- What data extraction patterns are you going to be using? 
- What data loading patterns are you going to be using? 
- What data transformation patterns are you going to be performing? 

## Breakdown of tasks 
How is your project broken down? Who is doing what?

--- NOTES FROM MIKE (2023-02-02)


Load (rds postgres) & Transform (seasonal?, monthly?, window functons

extract (consider unit test)
    As AWS offer a free tier, storing the data shouldn't cost you anything unless you amend the pipeline to extract large amounts of data

Transform (consider unit test)
    My transfrom is actually another pipeline of it's own, the goal is load data from database to another and then perform the transformation their.
    
Pipeline:
    All
    
Objective:
To show the impact of the pandemic on outbound flights in 3 international airports and 
compare with current situation. 

Consumers:
Data Analysts

Questions:
How many flights depart the airports within the time frames?
which of the 3 airports had the worst impact?

Datasets:
