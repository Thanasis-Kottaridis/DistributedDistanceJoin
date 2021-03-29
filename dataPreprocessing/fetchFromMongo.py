from pip._vendor.distlib.compat import raw_input
from utils import mongoConnector as connector
import csv
import json
import time


def findPolyFromSeas(seaName="Celtic Sea") :
    start_time = time.time()

    # connecting or switching to the database
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.world_seas

    results = collection.find_one({"properties.NAME" : seaName})
    print("--- %s seconds ---" % (time.time() - start_time))

    return results


def fetchPointInConstraint(seaName=None, collection=None,):
    pipeline = []

    start_time = time.time()

    if collection is None :
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    # get polygon
    if seaName is not None:
        poly = findPolyFromSeas()
        # add match arrg to pipeline
        pipeline.append(
            {
                "$match" : {
                    "location" : {"$geoWithin" :
                                      {"$geometry" : poly["geometry"]['coordinates'][0]}
                                  }
                }
            }
        )

    #add progect argg to pipeline
    # pipeline.append(
    #     {"$project" :
    #         {"_id" : 1,
    #          "X" : {"$arrayElemAt" : ["$location.coordinates", 0]},
    #          "Y" : {"$arrayElemAt" : ["$location.coordinates", 1]}}
    #         }
    #     )

    results = collection.aggregate(pipeline)

    dictlist = list(results)
    print("--- %s seconds ---" % (time.time() - start_time))

    print(json.dumps(dictlist, sort_keys=False, indent=4, default=str))

    return dictlist


def fetchPointsInTimeInterval(timeFrom=1448988894, timeTo=1451670894, logResponse=False):
    start_time = time.time()

    # connecting or switching to the database
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation

    pipeline = [
            {"$match" : {'ts' : {"$gte" : timeFrom, "$lte" : timeTo}}},
            {"$project" :
                 {"_id" : 1,
                  "X" : {"$arrayElemAt" : ["$location.coordinates", 0]},
                  "Y" : {"$arrayElemAt" : ["$location.coordinates", 1]}
                  }
             },
            # {
            #     "$count" : "total"
            # }
        ]

    results = collection.aggregate(pipeline)

    dictlist = list(results)
    print("--- %s seconds ---" % (time.time() - start_time))

    if logResponse:
        print(json.dumps(dictlist, sort_keys=False, indent=4, default=str))

    return dictlist


def csvToTxt():
    csv_file = raw_input('../testData/ais_one_hour.csv')
    txt_file = raw_input('../testData/ais_one_hour.txt')
    with open(txt_file, "w") as my_output_file:
        with open(csv_file, "r") as my_input_file:
            [my_output_file.write(" ".join(row)+'\n') for row in csv.reader(my_input_file)]
        my_output_file.close()


if __name__ == '__main__':
    csvToTxt()
