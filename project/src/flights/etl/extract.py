from pyflightdata import FlightData
import pandas as pd
import datetime as dt

class Extract():

    @staticmethod
    def extract_airport(
            airport_code:str=None
        )->pd.DataFrame:
        """
        Extracting data from the FlightRadar24 API. 
        - airport_code: code of the airport following the IATA convention
        """
        f=FlightData()
        response = f.get_airport_stats(airport_code,page=1,limit=10)
        
        if len(response) == 2: 
            data = response
        else: 
            raise Exception("Extracting aiport api data failed. Either no or limited data are available for the aiport. Please verify the airport code and try again later.")
        tmp = pd.json_normalize(data)
        tmp_selected = tmp[["departures.yesterday.quantity.onTime", 
                    "departures.yesterday.quantity.delayed",
                    "departures.yesterday.quantity.canceled",
                    "arrivals.yesterday.quantity.onTime", 
                    "arrivals.yesterday.quantity.delayed",
                    "arrivals.yesterday.quantity.canceled",
                    ]]
        tmp_selected = tmp_selected.rename(columns={'departures.yesterday.quantity.onTime': 'departures_onTime',
                             'departures.yesterday.quantity.delayed': 'departures_delayed',
                             'departures.yesterday.quantity.canceled': 'departures_canceled',
                             'arrivals.yesterday.quantity.onTime': 'arrivals_onTime',
                             'arrivals.yesterday.quantity.delayed': 'arrivals_delayed',
                             'arrivals.yesterday.quantity.canceled': 'arrivals_canceled'
                             })
        # include the airport code in the result data frame
        tmp_selected["airport_code"] = airport_code
        # include the pull date of the API call  
        tmp_selected["pull_date"] = dt.datetime.now().strftime("%Y-%m-%d")
        df_airport = tmp_selected
        return df_airport
    
    @staticmethod
    def extract_airport_list(
            fp_airports:str,
        )->pd.DataFrame:
        """
        Perform extraction using a filepath which contains a list of airports. 
        - fp_airports: filepath to a CSV file containing a list of airports 
        """

        # read list of airports
        # file location: "etl_draft/data/airports.csv"
        df_airports = pd.read_csv(fp_airports)
        # request data for each airport, combine data and reorder columns for output 
        df_concat = pd.DataFrame()
        for airport_code in df_airports["airport_code"]:
            df_extracted = Extract.extract_airport(airport_code=airport_code)
            df_concat = pd.concat([df_concat,df_extracted])
        # re-order columns: col1 = pull-date, col2= airport-code    
        df_concat = df_concat.iloc[:, [7, 6, 1, 2, 3, 4, 5]]
        return df_concat.reset_index().drop(labels=["index"], axis=1)