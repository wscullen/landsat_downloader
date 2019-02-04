""" Utilities for working with Sentinel2 and Landsat8 footprints

This is a series of functions for:

# converting between WRS and MGRS tile formats
# converting an arbitrary polygon into the equivalent MGRS tiles
# converting an arbitrary polygon into the equivalent WRS2 tiles

# 1st function, find the MGRS 100000m grid cells that intersect the polygon

# 2nd function, load the shapefile representations of those grid cells,
#   check which of those smaller grid cells intersect the polygon

"""
import os
from pathlib import Path
import json
import zipfile
import argparse
import utilities

# Testing submodule functionality in git


from osgeo import ogr, osr

ogr.UseExceptions()

GRID_DIR = Path(os.path.dirname(os.path.abspath(__file__)), "grid_files")

print(GRID_DIR)


def parse_args():
    """ Get the cmd line arguments using "argparse" library.

    """

    parser = argparse.ArgumentParser(
                description='')

    parser.add_argument('-shapefile',
                        dest="shapefile", action='store',
                        type=str,
                        help='Path to the shapefile you want to determine WRS intersections with.')

    parser.add_argument('-wrs_intersects',
                        dest="wrs_intersects", action='store_true',
                        help='Determine the WRSs that intersect the given shapefile')

    parser.add_argument('-wrs_to_mgrs',
                        dest="wrs_to_mgrs", action='store_true',
                        help='Create a csv lookup for wrs to mgrs')

    arg_object = parser.parse_args()

    return arg_object


def find_shapefile_wrs_intersections(path_to_shapefile):
    """Return (or write to file) the list of WRS path rows that intersect the given shapefile
    """

    # 1. Load shapefile
    # 2. If multiple features, union cascade to get just the outline
    # 3. Iterate over each wrs, test intersection with shapefile, if intersects add the field name to a list
    # 4. Write out list to a file

    shapefile_driver = ogr.GetDriverByName("ESRI Shapefile")

    wrs2_grid_dir = Path(GRID_DIR, 'WRS2_descending')
    wrs2_master_shp_file = Path(wrs2_grid_dir, 'WRS2_descending.shp')

    grid_ds = shapefile_driver.Open(str(wrs2_master_shp_file), 0)
    input_ds = shapefile_driver.Open(path_to_shapefile, 0)

    # Check if input_ds has multiple features, if so, union cascade it to flatten it (merge)
    # Create the feature and set values
    in_layer = input_ds.GetLayer()

    featureDefn = in_layer.GetLayerDefn()
    feature = ogr.Feature(featureDefn)

    multipoly = ogr.Geometry(ogr.wkbMultiPolygon)

    for feature in in_layer:
        print(feature.GetGeometryRef().GetGeometryName())
        geom = feature.GetGeometryRef()
        geom_type = geom.GetGeometryName()

        if geom_type == 'POLYGON':
            multipoly.AddGeometry(feature.GetGeometryRef())
        elif geom_type == 'MULTIPOLYGON':
            for geom_part in geom:
                if geom_part.GetGeometryName() == 'POLYGON':
                    multipoly.AddGeometry(geom_part)
                else:
                    print('unknown geom')
                    print(geom_part.GetGeometryName())

    cascade_union = multipoly.UnionCascaded()

    feature.SetGeometry(cascade_union)

    grid_layer = grid_ds.GetLayer()

    intersect_list = []

    # multipoly_intersect = ogr.Geometry(ogr.wkbMultiPolygon)

    for f in grid_layer:
        geom = f.GetGeometryRef()

        intersect_result = geom.Intersection(cascade_union)

        if not intersect_result.IsEmpty():
            # multipoly_intersect.AddGeometry(geom)
            print("FOUND INTERSECT")
            print(f.GetField('PR'))
            print(geom.GetGeometryName())
            intersect_list.append((f.GetField('PR'), geom.ExportToWkb()))

    print('DONE FINDING INTERSECTS')
    print(intersect_list)

    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(4326)

    # Create the output Driver
    # out_driver = ogr.GetDriverByName('ESRI')

    # Create the output GeoJSON
    out_datasource = shapefile_driver.CreateDataSource('intersecting_wrstiles.shp')
    # out_layer = out_datasource.CreateLayer('wrs', geom_type=ogr.wkbPolygon )

    out_layer = out_datasource.CreateLayer("wrs", spatial_ref, geom_type=ogr.wkbMultiPolygon)

    # Add an ID field
    idField = ogr.FieldDefn("id", ogr.OFTInteger)

    out_layer.CreateField(idField)

    pathrow_field = ogr.FieldDefn("pr", ogr.OFTString)
    path_field = ogr.FieldDefn("path", ogr.OFTString)
    row_field = ogr.FieldDefn("row", ogr.OFTString)

    out_layer.CreateField(pathrow_field)
    out_layer.CreateField(path_field)
    out_layer.CreateField(row_field)

    featureDefn = out_layer.GetLayerDefn()

    for idx, feat in enumerate(intersect_list):
        print('Tryng to create a feature.')
        feature = ogr.Feature(featureDefn)

        feature.SetField("id", idx + 1)
        feature.SetField("pr", feat[0])
        feature.SetField("path", feat[0][:3])
        feature.SetField("row", feat[0][3:])
        print('trying to set geometry')
        feature.SetGeometry(ogr.CreateGeometryFromWkb(feat[1]))
        # print(feat)

        out_layer.CreateFeature(feature)
        print('created feature')
        feature = None



    out_datasource = None

    # all done!
    grid_ds = None
    input_ds = None

    print(intersect_list)

    return intersect_list




def find_mgrs_intersection_large(footprint):
    """ Given a WKT polygon, return the list of MGRS tiles that intersect it

    the MGRS grid names retured are (generally) 6 x 8 degrees, each with
    a GZD (grid-zone designation) of AQ

    There are 1200 grid files to iterate over to check intersection

    A master MGRS GZD file has all GZD's, once the overall GZD's are known,
    use the individual shapefiles to find the fine-grained 100km tiles
    (ex:60WUT)

    Steps:
    1. Find intersections between footprint and master mgrs shp
    2. Unzip and load MGRS shp files based on the list generated in 1.
    3. Find intersections b/w footprint and the loaded shapefiles from 2.
    4. Return list of MGRS tiles that overlap with the given footprint

    """

    polygon_geom = ogr.CreateGeometryFromWkt(footprint)

    mgrs_grid_file_dir = Path(DATA_DIR, 'MGRS_100kmSQ_ID')

    mgrs_master_shp_file = Path(mgrs_grid_file_dir, 'mgrs_gzd_final.shp')

    shapefile_driver = ogr.GetDriverByName("ESRI Shapefile")

    print(mgrs_master_shp_file)

    grid_ds = shapefile_driver.Open(str(mgrs_master_shp_file), 0)

    layer = grid_ds.GetLayer()

    # sourceSR = layer.GetSpatialRef()
    # targetSR = osr.SpatialReference()
    # targetSR.ImportFromEPSG(4326) # WGS84
    # coordTrans = osr.CoordinateTransformation(sourceSR, targetSR)

    feature_count = layer.GetFeatureCount()
    print(f"Number of features in {os.path.basename(mgrs_master_shp_file)}: {feature_count}")
    layerDefinition = layer.GetLayerDefn()

    for i in range(layerDefinition.GetFieldCount()):
        print(layerDefinition.GetFieldDefn(i).GetName())

    intersect_list = []

    for f in layer:
        # print feature.GetField("STATE_NAME")
        # geom = feature.GetGeometryRef()
        # print(geom.Centroid().ExportToWkt())
        # geom_list.append(geom)
        # print(geom)

        geom = f.GetGeometryRef()
        # print(geom)
        # print(polygon_geom)
        intersect_result = geom.Intersection(polygon_geom)
        # print(intersect_result)
        if not intersect_result.IsEmpty():
            print("FOUND INTERSECT")
            print(f.GetField('gzd'))
            intersect_list.append(f.GetField('gzd'))

    # # iterate over the geometries and dissolve all into one
    # layer = dataSource.GetLayer()
    # layerDefinition = layer.GetLayerDefn()

    # for i in range(layerDefinition.GetFieldCount()):
    #     print(layerDefinition.GetFieldDefn(i).GetName())

    # Collect all Geometry
    # geomcol = ogr.Geometry(ogr.wkbGeometryCollection)
    # Create the feature and set values
    # featureDefn = outLayer.GetLayerDefn()
    # feature = ogr.Feature(featureDefn)

    # multipoly = ogr.Geometry(ogr.wkbMultiPolygon)
    # for feature in inLayer:
    #     geomcol.AddGeometry(feature.GetGeometryRef())

    # geom_list = []


        # geomcol.AddGeometry(geom)

        # if problem_child:
        #     # print(file_stem_split)
        #     # print(geom)
        #     print(geom.IsValid())
        #     # print(idx)
        #     pass
        # if file_stem_split in ['59Q']:
        #     print(geom)
        #     print(geom.GetGeometryName())




    # spatial_ref = osr.SpatialReference()
    # spatial_ref.ImportFromEPSG(4326)

    # outLayer = outDataSource.CreateLayer("gzd_8x6_degree", spatial_ref, geom_type=ogr.wkbMultiPolygon)

    # # Add an ID field
    # idField = ogr.FieldDefn("id", ogr.OFTInteger)
    # outLayer.CreateField(idField)
    # gzdField = ogr.FieldDefn("gzd", ogr.OFTString)
    # outLayer.CreateField(gzdField)

    # for idx, file_name in enumerate(Path(target_dir).iterdir()):
    #     problem_child = False
    #     print(idx)
    #     if file_name.suffix == '.zip':
    #         # print(file_name)
    #         print('Found a zip file')
    #         file_name_only = file_name.name
    #         file_name_stem = file_name.stem
    #         # print(file_name_only)
    #         # print(file_name_stem)

    #         file_stem_split = file_name_stem.split('_')[-1]

    #         print(file_stem_split)

    #         # if file_stem_split not in ['59Q']:
    #         #     continue

    #         if file_stem_split in ['Antarctica', 'Arctic']:
    #             continue

    #         with zipfile.ZipFile(file_name, 'r') as zf:
    #             # zf.extractall('temp_unzip')
    #             # print(zf.namelist())
    #             # print(zf.infolist())


    #             actual_file_stem = ""
    #             for zip_info in zf.infolist():
    #                 print(zip_info.filename)

    #                 if zip_info.filename[-1] == '/':
    #                     continue

    #                 zip_info.filename = zip_info.filename.split('/')[-1]
    #                 if actual_file_stem == "":
    #                     actual_file_stem = zip_info.filename.split('.')[0]
    #                 zf.extract(zip_info, temp_zip_dir)

    #         if actual_file_stem != file_name_stem:
    #             file_name_stem = actual_file_stem

    #         file_path = Path(temp_zip_dir, file_name_stem + '.shp')

    #         print(file_path)

    #         dataSource = outDriver.Open(str(file_path), 0) # 0 means read-only. 1 means writeable.

    #         if dataSource is None:
    #             print(f'Could not open {file_path}')
    #         else:
    #             print(f'Opened {file_path}')
    #             layer = dataSource.GetLayer()

    #             sourceSR = layer.GetSpatialRef()
    #             targetSR = osr.SpatialReference()
    #             targetSR.ImportFromEPSG(4326) # WGS84

    #             coordTrans = osr.CoordinateTransformation(sourceSR, targetSR)

    #             if file_stem_split in ['01R', '01S', '01K', '01J', '01H']:
    #                 # print('WE GOT A PROBLEM CHILD HERE')embarassing
    #                 # print('\n\n\n\n')
    #                 problem_child = True


    #             featureCount = layer.GetFeatureCount()
    #             print(f"Number of features in {os.path.basename(file_path)}: {featureCount}")

    #             # # iterate over the geometries and dissolve all into one
    #             # layer = dataSource.GetLayer()
    #             # layerDefinition = layer.GetLayerDefn()

    #             # for i in range(layerDefinition.GetFieldCount()):
    #             #     print(layerDefinition.GetFieldDefn(i).GetName())

    #             # Collect all Geometry
    #             geomcol = ogr.Geometry(ogr.wkbGeometryCollection)
    #             # Create the feature and set values
    #             featureDefn = outLayer.GetLayerDefn()
    #             feature = ogr.Feature(featureDefn)

    #             multipoly = ogr.Geometry(ogr.wkbMultiPolygon)
    #             # for feature in inLayer:
    #             #     geomcol.AddGeometry(feature.GetGeometryRef())

    #             # geom_list = []
    #             for f in layer:
    #                 # print feature.GetField("STATE_NAME")
    #                 # geom = feature.GetGeometryRef()
    #                 # print(geom.Centroid().ExportToWkt())
    #                 # geom_list.append(geom)
    #                 # print(geom)

    #                 geom = f.GetGeometryRef()
    #                 geom.Transform(coordTrans)

    #                 # geomcol.AddGeometry(geom)

    #                 if problem_child:
    #                     # print(file_stem_split)
    #                     # print(geom)
    #                     print(geom.IsValid())
    #                     # print(idx)
    #                     pass
    #                 if file_stem_split in ['59Q']:
    #                     print(geom)
    #                     print(geom.GetGeometryName())

    #                 if idx > 18 and idx < 1176:
    #                     if file_stem_split in ['59Q']:
    #                         print(geom)
    #                         print(geom.GetGeometryName())
    #                     if geom.GetGeometryName() == 'MULTIPOLYGON':
    #                        for i in range(0, geom.GetGeometryCount()):
    #                             g = geom.GetGeometryRef(i)
    #                             # print(g.GetGeometryName())
    #                             multipoly.AddGeometry(g)


    #                     else:
    #                         multipoly.AddGeometry(geom)
    #                 elif idx > 1176:
    #                     sub_geom = geom.GetGeometryRef(0)
    #                     # print(sub_geom)
    #                     # print(sub_geom.GetPointCount())

    #                     outRing = ogr.Geometry(ogr.wkbLinearRing)
    #                     # outRing.AddPoint(1154115.274565847, 686419.4442701361)

    #                     # Create inner ring
    #                     # innerRing = ogr.Geometry(ogr.wkbLinearRing)
    #                     # innerRing.AddPoint(1149490.1097279799, 691044.6091080031)

    #                     # Create polygon
    #                     poly = ogr.Geometry(ogr.wkbPolygon)
    #                     # poly.AddGeometry(outRing)
    #                     # poly.AddGeometry(innerRing)

    #                     for i in range(0, sub_geom.GetPointCount()):
    #                         # GetPoint returns a tuple not a Geometry
    #                         pt = sub_geom.GetPoint(i)

    #                         if pt[0] < 0:
    #                             point_x = pt[0] * -1
    #                             point_y = pt[1]
    #                         else:
    #                             point_x = pt[0]
    #                             point_y = pt[1]

    #                         outRing.AddPoint(point_x, point_y)
    #                         # print("%i). POINT (%f %f)" %(i, point_x, point_y))

    #                     poly.AddGeometry(outRing)



    #                     multipoly.AddGeometry(poly)
    #                 else:
    #                     sub_geom = geom.GetGeometryRef(0)
    #                     # print(sub_geom)
    #                     # print(sub_geom.GetPointCount())

    #                     outRing = ogr.Geometry(ogr.wkbLinearRing)
    #                     # outRing.AddPoint(1154115.274565847, 686419.4442701361)

    #                     # Create inner ring
    #                     # innerRing = ogr.Geometry(ogr.wkbLinearRing)
    #                     # innerRing.AddPoint(1149490.1097279799, 691044.6091080031)

    #                     # Create polygon
    #                     poly = ogr.Geometry(ogr.wkbPolygon)
    #                     # poly.AddGeometry(outRing)
    #                     # poly.AddGeometry(innerRing)

    #                     for i in range(0, sub_geom.GetPointCount()):
    #                         # GetPoint returns a tuple not a Geometry
    #                         pt = sub_geom.GetPoint(i)

    #                         if pt[0] > 0:
    #                             point_x = pt[0] * -1
    #                             point_y = pt[1]
    #                         else:
    #                             point_x = pt[0]
    #                             point_y = pt[1]

    #                         outRing.AddPoint(point_x, point_y)
    #                         # print("%i). POINT (%f %f)" %(i, point_x, point_y))

    #                     poly.AddGeometry(outRing)



    #                     multipoly.AddGeometry(poly)

    #             # layer.ResetReading()

    #             # print(feature)

    #             cascade_union = multipoly.UnionCascaded()
    #             feature.SetGeometry(cascade_union)

    #             # convexhull = geomcol.ConvexHull()
    #             # feature.SetGeometry(convexhull)

    #             file_name_split = file_name_stem.split('_')
    #             gzd_from_file_name = file_name_split[-1]
    #             # print(idx + 1)

    #             feature.SetField("id", idx + 1)
    #             feature.SetField("gzd", gzd_from_file_name)

    #             outLayer.CreateFeature(feature)

    #             feature = None

    #             # Save and close DataSource
    #             dataSource = None

    #             for file_name in Path(temp_zip_dir).iterdir():
    #                 os.remove(file_name)

    #         # if idx > 20:
    #         #     break

    # # all done!
    grid_ds = None

    return intersect_list


def find_mgrs_intersection_100km(footprint, gzd_list):
    """
    Given a WKT polygon, return the list of MGRS 100km grids that intersect it

    Utilize the helper function find_mgrs_intersection_single for each GZD
    """

    total_mgrs_100km_list = []

    for gzd in gzd_list:
        sub_list = find_mgrs_intersection_100km_single(footprint, gzd)
        for mgrs_id in sub_list:
            total_mgrs_100km_list.append(mgrs_id)

    return total_mgrs_100km_list


def find_mgrs_intersection_100km_single(footprint, gzd):
    """
    Given a WKT polygon and a GZD (grid zone designator)
    return the list of 100km MGRS gzd that intersect the WKT polygon

    Overview:
    1. Based on the GZD, unzip the matching .shp and load
    2. Run interesction check on each feature of the .shp
    3. Save intersections to a list, the field is 100kmSQ_ID
    4. Clean up unziped files, return list of intersecting 100kmSQ_ID's

    """

    polygon_geom = ogr.CreateGeometryFromWkt(footprint)

    zip_name = f'MGRS_100kmSQ_ID_{gzd}.zip'
    file_name_stem = f'MGRS_100kmSQ_ID{gzd}'
    full_zip_path = Path(DATA_DIR, 'MGRS_100kmSQ_ID', zip_name)

    # 1. unzip the appropriate shapefile
    with zipfile.ZipFile(full_zip_path, 'r') as zf:
        actual_file_stem = ""
        for zip_info in zf.infolist():
            print(zip_info.filename)

            if zip_info.filename[-1] == '/':
                continue

            zip_info.filename = zip_info.filename.split('/')[-1]

            if actual_file_stem == "":
                actual_file_stem = zip_info.filename.split('.')[0]

            # Extract only the files to a specific dir
            zf.extract(zip_info, DATA_DIR)

    if actual_file_stem != file_name_stem:
        file_name_stem = actual_file_stem

    file_path = Path(DATA_DIR, file_name_stem + '.shp')

    # 2. Load the shp file and run intersection check on each feature
    shapefile_driver = ogr.GetDriverByName("ESRI Shapefile")

    grid_ds = shapefile_driver.Open(str(file_path), 0)

    layer = grid_ds.GetLayer()

    # transform coords from local UTM proj to lat long
    sourceSR = layer.GetSpatialRef()
    targetSR = osr.SpatialReference()
    targetSR.ImportFromEPSG(4326) # WGS84
    coordTrans = osr.CoordinateTransformation(sourceSR, targetSR)

    intersect_list = []

    for f in layer:
        geom = f.GetGeometryRef()
        geom.Transform(coordTrans)

        intersect_result = geom.Intersection(polygon_geom)

        if not intersect_result.IsEmpty():
            print("FOUND INTERSECT")
            print(f.GetField('100kmSQ_ID'))
            intersect_list.append(f'{gzd}{f.GetField("100kmSQ_ID")}')

    # all done!
    grid_ds = None

    # clean up
    for file_name in Path(DATA_DIR).iterdir():
        if file_name.is_file():
            os.remove(file_name)

    return intersect_list


def filter_by_footprint(footprint, list_of_results, dataset_name):
    """
    Given a footprint in wkt, remove non intersecting products by using
    their spatial footprint.

    In the case of the USGS EE S2, spatialFootprint is a geojson object.

    1. Create geometry ref using ogr for footprint
    2. Iterate over each product, create geometry ref for each product
        (some will be geojson, some GML, some wkt)
    3. If the footprint of the tile and the overall footprint intersect
        keep the product, otherwise get rid of it.
    """

    polygon_geom = ogr.CreateGeometryFromWkt(footprint)

    intersect_list = []
    for f in list_of_results:
        geom = ogr.CreateGeometryFromJson(json.dumps(f['spatialFootprint']))

        intersect_result = geom.Intersection(polygon_geom)

        if not intersect_result.IsEmpty():
            print("FOUND INTERSECT")
            intersect_list.append(f)
        else:
            print("NO INTERSECT")

    return intersect_list


if __name__ == "__main__":

    arg_obj = parse_args()

    result = None

    if arg_obj.wrs_intersects:
        result = find_shapefile_wrs_intersections(arg_obj.shapefile)
    elif arg_obj.wrs_to_mgrs:
        utilities.create_wrs_to_mgrs_lookup(arg_obj.shapefile)

    print(result)