# flight_pipelines

--- COPY FROM INSTRUCTIONS

# Project plan 

## Objective 
A short statement about the objective of your project 

## Consumers 
What users would find your data useful? How do they want to access the data? 

## Questions 
What questions are you trying to answer with your data? How will your data support your users?

## Source datasets 
What datasets are you sourcing from? How frequently are the source datasets updating?

## Solution architecture
How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution? 

- What data extraction patterns are you going to be using? 
- What data loading patterns are you going to be using? 
- What data transformation patterns are you going to be performing? 

## Breakdown of tasks 
How is your project broken down? Who is doing what?

--- NOTES FROM MIKE (2023-02-02)

Extract data from Flights (outbound) API for 3 airports from 2020-current_date
Load (rds postgres) & Transform (seasonal?, monthly?, window functons
extract (consider unit test)
    GG/Mike
Transform (consider unit test)
    Marc
    
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
