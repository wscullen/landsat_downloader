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
import csv


from osgeo import ogr, osr

ogr.UseExceptions()

DATA_DIR = Path(os.path.dirname(os.path.abspath(__file__)), "grid_files")

print(DATA_DIR)

def create_wrs_to_mgrs_lookup(wrs_shapefile):
    """Use the Canada only WRS shapefile to find all MGRS that overlap with each WRS

    Write out to csv.

    # Load canada WRS
    # iterate over features
    # pull geom ref from each feature
    # call find_mgrs_intersection_large on each footprint
    # create a tuple for each feature, add the PathRow, MGRS_List
    # write out csv at the end

    """

    shapefile_driver = ogr.GetDriverByName("ESRI Shapefile")

    grid_ds = shapefile_driver.Open(wrs_shapefile, 0)

    layer = grid_ds.GetLayer()


    path_row_list = []

    total_features = layer.GetFeatureCount()

    for idx, f in enumerate(layer):

        print(f'{idx} of {total_features}')

        footprint = f.GetGeometryRef().ExportToWkt()
        pathrow = f.GetField('PR')


        mgrs_list = find_mgrs_intersection_large(footprint)
        print(mgrs_list)
        mgrs_list_fine = []

        mgrs_list_fine += find_mgrs_intersection_100km(footprint, mgrs_list)

        print('for path row')
        print(pathrow)
        print(mgrs_list)
        print(mgrs_list_fine)
        print('\n\n')
        path_row_list.append((str(pathrow), ' '.join(mgrs_list_fine)))

    with open('wrs_to_mgrs.csv','w', newline='') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['pathrow','mgrs_list'])

        for row in path_row_list:
            csv_out.writerow(row)



def get_usgs_detailed_metadata_field(result, field_name):
    result_list = [val['value'] for val in result['detailed_metadata'] if val['fieldName'] == field_name]
    if len(result_list) != 0:
        return result_list[0]
    else:
        return None

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
    #                 # print('WE GOT A PROBLEM CHILD HERE')
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

def unzip_mgrs_shapefile(gzd):
    print(gzd)
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

    return file_path

def clean_data_dir():
    for file_name in Path(DATA_DIR).iterdir():
        if file_name.is_file():
            os.remove(file_name)

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

    file_path = unzip_mgrs_shapefile(gzd)

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

    clean_data_dir()

    return intersect_list

def find_wrs_intersection(footprint):
    """Find all WRS path rows that the given footprint intersects with"""
    polygon_geom = ogr.CreateGeometryFromWkt(footprint)

    file_path = Path(DATA_DIR, 'WRS2_descending', 'WRS2_descending.shp')

    # 2. Load the shp file and run intersection check on each feature
    shapefile_driver = ogr.GetDriverByName("ESRI Shapefile")

    grid_ds = shapefile_driver.Open(str(file_path), 0)

    layer = grid_ds.GetLayer()

    # transform coords from local UTM proj to lat long
    # sourceSR = layer.GetSpatialRef()
    # targetSR = osr.SpatialReference()
    # targetSR.ImportFromEPSG(4326) # WGS84
    # coordTrans = osr.CoordinateTransformation(sourceSR, targetSR)

    intersect_list = []

    for f in layer:
        geom = f.GetGeometryRef()
        # geom.Transform(coordTrans)

        intersect_result = geom.Intersection(polygon_geom)

        if not intersect_result.IsEmpty():
            print("FOUND INTERSECT")
            print(f.GetField('PR'))
            intersect_list.append((f.GetField("PR")[:3], f.GetField("PR")[3:]))

    # all done!
    grid_ds = None

    return intersect_list

def get_mgrs_footprint(mgrs_id):
    """Given a MGRS ID ()
    mgrs_id = ZONE NUMBER, BAND LETTER, 100km designation

    return the WKT polygon of the zone footprint
    """

    gzd = mgrs_id[:3]

    mgrs_100km_gzd = mgrs_id[3:]
    print(gzd)
    print(mgrs_100km_gzd)

    file_path = unzip_mgrs_shapefile(gzd)

    shapefile_driver = ogr.GetDriverByName("ESRI Shapefile")

    grid_ds = shapefile_driver.Open(str(file_path), 0)

    layer = grid_ds.GetLayer()

    # transform coords from local UTM proj to lat long
    sourceSR = layer.GetSpatialRef()
    targetSR = osr.SpatialReference()
    targetSR.ImportFromEPSG(4326) # WGS84
    coordTrans = osr.CoordinateTransformation(sourceSR, targetSR)

    intersect_list = []
    wkt_footprint = None
    for f in layer:

        if f.GetField('100kmSQ_ID') == mgrs_100km_gzd:
            geom = f.GetGeometryRef()
            geom.Transform(coordTrans)
            wkt_footprint = geom.ExportToWkt()

    # all done!
    grid_ds = None

    clean_data_dir()

    return wkt_footprint



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
    print('running as a command line tool')
    print('Get WRS list from a shapefile (1)')
    print('Get all MGRS from a list of WRS (2)')

    user_choice = input('What would you like to do?')