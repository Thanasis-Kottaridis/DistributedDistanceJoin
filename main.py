# Main  Imports
import time
import sys

# Local App Imports
from utils import spatialUtils as utils

# Spark Imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Python math imports
from math import radians, cos, sin, asin, sqrt


def calculate_minimum_grid_from_sdf(sdf1, sdf2, theta=10):
    """
    This Helper func takes 2 spark dataframe and:
     1) founds their MBB's using calculate_MBB_from_sdf helper func
     2) then compairs the sum of each MBB dx and dy and selects the MBB the minimum
     3) Calculates the grid for the minimum MBB
    :param sdf1: first pointset dataframe
    :param sdf2: second pointset dataframe
    :param theta: distance theta given by user
    :return:
     - grid: For the minimum MBB
     - isPointSetA: A boolean flag tha indicates if the minimum MBB is for pointset A or not
    """
    mbb_row1, dx1, dy1 = calculate_MBB_from_sdf(sdf1)
    mbb_row2, dx2, dy2 = calculate_MBB_from_sdf(sdf2)

    if (dx1 * dy1) <= (dx2 * dy2):
        return calculate_grid_from_MBB(mbb_row1, theta=theta), True
    else:
        return calculate_grid_from_MBB(mbb_row2, theta=theta), False


def calculate_MBB_from_sdf(sdf):
    # find min/max X,Y
    mbb_row = sdf.agg(
        F.max(sdf.X),
        F.min(sdf.X),
        F.max(sdf.Y),
        F.min(sdf.Y),
    ).collect()[0]
    print("print mbb: {}".format(mbb_row))

    # calculate dx and dy
    dx = mbb_row["max(X)"] - mbb_row["min(X)"]
    dy = mbb_row["max(Y)"] - mbb_row["min(Y)"]

    return mbb_row, dx, dy


def calculate_grid_from_MBB(mbb_row, theta):
    # calculate grid for dataset
    return utils.getPolyGrid(mbb_row["min(X)"],
                             mbb_row["min(Y)"],
                             mbb_row["max(X)"],
                             mbb_row["max(Y)"], theta=theta)


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r


if __name__ == '__main__' :
    """
        Excequte distributed distance join:
        1) first fetch the data and theta from users input
        2) Compare MBB's from each dataset and create grid from the smaller one
        3) perform spatial join without using intersection. use where statement on join and check if point is within grid box

    """

    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))

    # Constants
    # test paths
    test_sample_A_path = sys.argv[1] if len(sys.argv) == 6 is not None else "testData/ais_one_hour_25k.csv"
    test_sample_B_path = sys.argv[2] if len(sys.argv) == 6 is not None else "testData/ais_one_hour2_25k.csv"

    # test theta
    theta = float(sys.argv[3]) if len(sys.argv) == 6 is not None else 10
    distanceType = sys.argv[4] if len(sys.argv) == 6 is not None else "euclidean"
    enviromentType = sys.argv[5] if len(sys.argv) == 6 is not None else "local"

    # full program timer
    total_time = time.time()

    # create spark session object

    if enviromentType == "cluster":
        spark = SparkSession.builder \
            .appName("DistributedDistanceJoin_Test") \
            .master("spark://192.168.0.2:7077") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .getOrCreate()
    else:
        spark = SparkSession.builder \
            .appName("DistributedDistanceJoin_Test") \
            .master("local").getOrCreate()


    # Load CSV Files
    df_a = spark.read.csv(header="True", inferSchema="True", path=test_sample_A_path)
    df_b = spark.read.csv(header="True", inferSchema="True", path=test_sample_B_path)

    # TODO REMOVE FILTERING!
    # df_a = df_a.filter((-6.5 <= df_a.X) & (df_a.X <= -5) & (47.5 <= df_a.Y) & (df_a.Y <= 49)) \
    #     .cache()
    #
    # df_b = df_b.filter((-6 <= df_b.X) & (df_b.X <= -5) & (47.5 <= df_b.Y) & (df_b.Y <= 49)) \
    #     .cache()

    # df_a = df_a.limit(25000).cache()
    # df_b = df_b.limit(75000).cache()


    print("Total filtered_df_a count {}".format(df_a.count()))
    df_a.show(10)
    print("Total filtered_df_b count {}".format(df_b.count()))
    df_b.show(10)

    # Grid dataset with minimum MBB
    create_grid_time = time.time()

    grid, isPointSetA = calculate_minimum_grid_from_sdf(df_a, df_b, theta=theta)

    # log grid generation stats
    print("grid len: {}".format(len(grid)))
    print("--- %s seconds ---" % (time.time() - create_grid_time))

    # swap df_a with df_b if needed:
    # this block of code will run if dataset B has smaller MBB from A because we want the df_a to be
    # the smallest MBB dataset
    if not isPointSetA:
        temp_df = df_b
        df_b = df_a
        df_a = temp_df

    # Create Grid RDD or DF
    grid_df = spark.createDataFrame(grid)

    # perform spatial join grids with pointSet A
    # use broadcasting in order to send grid dataframe
    # (which is small les than 100k grids in most cases) to all cluster nodes
    start_time = time.time()

    spatial_join_result = df_a.join(F.broadcast(grid_df), (
        (df_a.X >= grid_df.grid_minX) &
        (df_a.X <= grid_df.grid_maxX) &
        (df_a.Y >= grid_df.grid_minY) &
        (df_a.Y <= grid_df.grid_maxY)))\
        .cache()  # cache results to avoid perform join again

    print("--- %s seconds ---" % (time.time() - start_time))

    # find pings per grid
    filtered_grid = spatial_join_result.groupby(
        ["grid_id", "grid_maxX", "grid_maxY", "grid_minX", "grid_minY", "exp_grid_maxX", "exp_grid_maxY",
         "exp_grid_minX", "exp_grid_minY"]) \
        .count() \
        .filter(F.col("count") > 0) \
        .cache()  # cache filtered grid

    # Perform spatial join with pointset B and filtered expanded grids
    # we use broadcasting again in order to send filtered grids to all nodes

    # timing procedure
    start_time = time.time()

    spatial_join_b_result = df_b.join(F.broadcast(filtered_grid), (
        (df_b.X >= filtered_grid.exp_grid_minX) &
        (df_b.X <= filtered_grid.exp_grid_maxX) &
        (df_b.Y >= filtered_grid.exp_grid_minY) &
        (df_b.Y <= filtered_grid.exp_grid_maxY)))\
        .withColumnRenamed("grid_id", "exp_grid_id")\
        .withColumn("grid_id", F.when((F.col("X") >= F.col("grid_minX")) &
                                   (F.col("X") <= F.col("grid_maxX")) &
                                   (F.col("Y") >= F.col("grid_minY")) &
                                   (F.col("Y") <= F.col("grid_maxY")), F.col("exp_grid_id"))
                 .otherwise(None)) \
        .cache()  # cache results to avoid perform join again

    print("--- %s seconds ---" % (time.time() - start_time))

    # Repartition spatial_join_result based on grid_id and spatial_join_b_result based on exp_grid_id
    # partitions_count = spatial_join_b_result.rdd.getNumPartitions()
    # spatial_join_b_result = spatial_join_b_result.repartition(partitions_count, "exp_grid_id")\
    #     .persist()

    # Perform a simple join on grid_id column on spatial_join_result and spatial_join_b_result
    # in order to find all points in the same grid (those points consider matching by default)
    # timing procedure
    start_time = time.time()

    filtered_a_results = spatial_join_result \
        .select(F.col("_id").alias("a_id"),
                F.col("X").alias("a_X"),
                F.col("Y").alias("a_Y"),
                F.col("grid_id").alias("a_grid_id"))\
        .cache()

    ta = filtered_a_results.alias('ta')

    print("--- %s seconds ---" % (time.time() - start_time))

    # Find matching pairs tha points from A are in grid and poins from B are in expanded and not in grid
    # declare UDF function for haversine distance
    udfsomefunc = F.udf(haversine, DoubleType())

    # timing procedure
    start_time = time.time()

    # register udf func
    haversine_dist_udf = F.udf(utils.calculatePointsDistance, DoubleType())



    filtered_b_in_exp_grid = spatial_join_b_result.where(F.col("grid_id").isNull()) \
        .select(F.col("_id").alias("b_id"),
                F.col("X").alias("b_X"),
                F.col("Y").alias("b_Y"),
                F.col("exp_grid_id").alias("b_exp_grid_id"))

    tb = filtered_b_in_exp_grid.alias('tb')

    """
        Using approximation that 1 degree on earth is 111km
    """
    if distanceType != "haversine":
        kmPerDegree = 1 / 111
        targetDist = kmPerDegree * theta
        final_result = ta.join(tb, ta.a_grid_id == tb.b_exp_grid_id)\
            .where(F.sqrt(pow(F.col("a_X") - F.col("b_X"), 2) + pow(F.col("a_Y") - F.col("b_Y"), 2)) <= targetDist)\
            .select(F.col("a_id"), F.col("b_id"))

    else:
        """
            Haversine using spark F functions
        """
        final_result = ta.join(tb, ta.a_grid_id == tb.b_exp_grid_id)\
            .withColumn("a", (
                F.pow(F.sin((F.radians(F.col("b_Y") - F.col("a_Y"))) / 2), 2) +
                F.cos(F.radians(F.col("a_Y"))) * F.cos(F.radians(F.col("b_Y"))) *
                F.pow(F.sin((F.radians(F.col("b_X") - F.col("a_X"))) / 2), 2)))\
            .withColumn("haversine_dist", F.asin(F.sqrt(F.col("a"))) * 12742)\
            .filter(F.col("haversine_dist") <= theta)\
            .select(F.col("a_id"), F.col("b_id"))

    """
        UDF Haversine distance
    """
    # final_result = ta.join(tb, ta.a_grid_id == tb.b_exp_grid_id)
    # final_result = final_result.withColumn("haversine_dist",
    #                                        udfsomefunc(F.col("a_X"), F.col("a_Y"), F.col("b_X"), F.col("b_Y"))) \
    #     .filter(F.col("haversine_dist") <= theta)


    # TODO WRITE DATA TO FILE
    # final_result.write.save("dist_join_result.csv", format="csv", mode="append")

    """
        Perform grid join
    """
    filtered_b_results = spatial_join_b_result \
        .filter(F.col("grid_id") == F.col("exp_grid_id")) \
        .select(F.col("_id").alias("b_id"), F.col("grid_id").alias("b_grid_id"))

    tb = filtered_b_results.alias('tb')

    first_result = ta.join(tb, ta.a_grid_id == tb.b_grid_id) \
        .select(F.col("a_id"), F.col("b_id"))

    # write file
    # print(first_result.count())  # TODO REMOVE THIS LINE
    # TODO WRITE THIS POINTS TO HDFS
    # .write.save("dist_join_result.csv", format="csv", mode="overwrite")

    # show explain
    # first_result.explain()
    # final_result.explain()

    print(first_result.count())  # TODO REMOVE THIS LINE
    print("--- %s seconds ---" % (time.time() - start_time))

    # timing procedure
    start_time = time.time()

    print(final_result.count())  # TODO REMOVE THIS LINE
    print("--- %s seconds ---" % (time.time() - start_time))


    first_result.union(final_result)\
        .write.csv('testData/dj_results')


    print("--- TOTAL EXECUTION TIME ---\n--- %s seconds ---" % (time.time() - total_time))
