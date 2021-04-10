# Main  Imports
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt

# Python shapely imports
import shapely.geometry as sg
from shapely.geometry import Polygon
import geog

"""
    Plot Utils
"""


def createAXNFigure() :
    """
    creates a map ploting european coastline
    :return: the ax
    """
    # geopandas basic world map with out details
    # world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world = gpd.read_file("../testData/EuropeanCoastline/Europe Coastline (Polygone).shp")
    world.to_crs(epsg=4326, inplace=True)  # convert axes tou real world coordinates

    ax = world.plot(figsize=(10, 6))
    # plt.axis([-9, 1, 45, 51])  # set plot bounds
    plt.axis([-7, -4, 47, 49.5])  # set zoom plot bounds
    return ax


def calculatePointsDistance(lon1, lat1, lon2, lat2) :
    """
    Vectorized helper func for calculating distance between 2 geo points

    source:
     https://towardsdatascience.com/heres-how-to-calculate-distance-between-2-geolocations-in-python-93ecab5bbba4

    :param lat1, lon1: lat long of first point
    :param lat2, lon2: lat log of second point
    :return: distance between points
    """

    r = 6371
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    res = r * (2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))
    return np.round(res, 2)
    # approach using geopy
    # return geopy.distance.geodesic(coords_1, coords_2).km


def calculateMBB_toPoly(Px, Py, d, polyPoints=5) :
    """
    :param Px: target point x coordinate
    :param Py: target point y coordinate
    :param d: the distance that we want to expand point on each side (in km)
    :param polyPoints: the shape of the poly that we want to produce
           @SOS: for square polyPoints = 5 (4+1 in order to create polygon.)
    :return bottomLeftMBB: the bottom left point of the mbb
    :return upperRightMBB: the upper right point of the mbb
    """
    p = sg.Point(Px, Py)
    angles = np.linspace(0, 360, polyPoints)
    polygon = geog.propagate(p, angles, d * 1000)

    return sg.Polygon(polygon)


def calculateMBB_toSP_PointsSet(Px, Py, d, polyPoints=5) :
    """
    :param Px: target point x coordinate
    :param Py: target point y coordinate
    :param d: the distance that we want to expand point on each side (in km)
    :param polyPoints: the shape of the poly that we want to produce
           @SOS: for square polyPoints = 5 (4+1 in order to create polygon.)
    :return bottomLeftMBB: the bottom left point of the mbb
    :return upperRightMBB: the upper right point of the mbb
    """
    p = sg.Point(Px, Py)
    angles = np.linspace(0, 360, polyPoints)
    polygon = geog.propagate(p, angles, d * 1000)

    # un zip x and y coords
    x_coordinates, y_coordinates = zip(*polygon)

    # ----------- Example To Rturn points ------------
    bottomLeftMBB = sg.Point(min(x_coordinates), min(y_coordinates))
    upperRightMBB = sg.Point(max(x_coordinates), max(y_coordinates))

    return bottomLeftMBB, upperRightMBB


def calculateMBB_toPoints_List(Px, Py, d, polyPoints=5) :
    """
    :param Px: target point x coordinate
    :param Py: target point y coordinate
    :param d: the distance that we want to expand point on each side (in km)
    :param polyPoints: the shape of the poly that we want to produce
           @SOS: for square polyPoints = 5 (4+1 in order to create polygon.)
    :return bottomLeftMBB: the bottom left point of the mbb
    :return upperRightMBB: the upper right point of the mbb
    """
    p = sg.Point(Px, Py)
    angles = np.linspace(0, 360, polyPoints)
    polygon = geog.propagate(p, angles, d * 1000)

    # un zip x and y coords
    x_coordinates, y_coordinates = zip(*polygon)

    # ----------- Example To Rturn Coord Tuples ------------
    bottomLeftMBB = [min(x_coordinates), min(y_coordinates)]
    upperRightMBB = [max(x_coordinates), max(y_coordinates)]

    return [bottomLeftMBB, upperRightMBB]


"""
    GRID UTILS
"""


def getPolyGrid(xmin, ymin, xmax, ymax, theta) :
    """
    Helper func for generating a grid for a target poly
    :param poly: target poly
    :param theta: grid square sidi length in meters
    :return: a grid geo dataframe
    """

    # we need a grid with diagonial of length theta
    d = theta/(2**(1/2))
    kmPerDegree = 1 / 111
    length = wide = kmPerDegree * d

    cols = list(np.arange(xmin, xmax + wide, wide))
    rows = list(np.arange(ymin, ymax + length, length))
    rows.reverse()

    polygons = []
    grid_id = 0
    for x in cols :
        for y in rows :
            gridPointLis = [(x, y), (x + wide, y), (x + wide, y - length), (x, y - length)]
            enrich_grid_minX, enrich_grid_maxX, enrich_grid_minY, enrich_grid_maxY = getEnrichBoundingBox(gridPointLis,
                                                                                                          theta)
            polygons.append({
                "grid_id" : grid_id,
                "grid_minX" : float(round(x, 5)),
                "grid_maxX" : float(round(x + wide, 5)),
                "grid_minY" : float(round(y - length, 5)),
                "grid_maxY" : float(round(y, 5)),
                "exp_grid_minX" : enrich_grid_minX,
                "exp_grid_maxX" : enrich_grid_maxX,
                "exp_grid_minY" : enrich_grid_minY,
                "exp_grid_maxY" : enrich_grid_maxY,
                # "expanded_grid_geometry": Polygon(gridPointLis),
                # "grid_geometry": Polygon(gridPointLis)
            })
            # update grid id for next grid
            grid_id += 1

    return polygons


def createPolyFromPoints(minX, maxX, minY, maxY) :
    pointsList = [(minX, maxY), (maxX, maxY), (maxX, minY), (minX, minY)]
    return Polygon(pointsList)


def getEnrichBoundingBox(pointsList, theta=0) :
    """
    This helper func calculates the enrich bounding box of a polygon.
    and returns its bottom left and upper right coordinates

    :param poly:
    :param theta:
    :return: the polygon geo json of bounding box
    """
    x_coordinates, y_coordinates = zip(*pointsList)

    MBB_points = [
        [min(x_coordinates), min(y_coordinates)],
        [min(x_coordinates), max(y_coordinates)],
        [max(x_coordinates), max(y_coordinates)],
        [max(x_coordinates), min(y_coordinates)],
        [min(x_coordinates), min(y_coordinates)],
    ]

    enritch_MBB_points_x = []
    enritch_MBB_points_y = []
    for point in MBB_points :
        d = theta * 1000  # meters
        p = sg.Point(point)
        n_points = 5
        angles = np.linspace(0, 360, n_points)
        polygon = geog.propagate(p, angles, d)

        xs, ys = zip(*polygon)

        enritch_MBB_points_x.extend([min(xs), max(xs)])
        enritch_MBB_points_y.extend([min(ys), max(ys)])

    return float(round(min(enritch_MBB_points_x), 5)), \
           float(round(max(enritch_MBB_points_x), 5)), \
           float(round(min(enritch_MBB_points_y), 5)), \
           float(round(max(enritch_MBB_points_y), 5))
