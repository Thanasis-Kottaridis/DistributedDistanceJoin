from dataPreprocessing import fetchFromMongo
import pandas as pd

# Constants
one_month_Uinix = 2682000
one_hour_in_Unix = 3600


def getOneMonthData(fromTime=1448988894) :
    results = fetchFromMongo.fetchPointsInTimeInterval(timeFrom=fromTime, timeTo=fromTime + 2682000)
    points_df = pd.DataFrame(results)
    points_df.to_csv('ais_one_month.csv')


def getOneHourData(fromTime=1448988894) :
    results = fetchFromMongo.fetchPointsInTimeInterval(timeFrom=fromTime, timeTo=fromTime + 24*one_hour_in_Unix)
    points_df = pd.DataFrame(results)
    points_df.to_csv('ais_one_hour2.csv')


# Press the green button in the gutter to run the script.
if __name__ == '__main__' :
    # getOneMonthData()
    getOneHourData(1448988894 + (24*one_hour_in_Unix))
