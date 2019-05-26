## Overview

This is a helper module that interacts with the USGS Earth Explorer website for downloading L8 and S2 satellite imagery.

## Extra Utilities

Also included are some extra utility scripts used for converting between WRS and MGRS tile systems. Use the `shapefile_to_wrs.py` script to create a shapefile with all the WRS tiles that overlap with a given shapefile. You can use this WRS shapefile to generate a .csv lookup table going from WRS pathrow to a list of MGRS tiles.

Generate a WRS intersection with arbirtary shapefile:
`python shapefile_to_wrs.py -shapefile ./data/canada_extent.shp -wrs_intersects`

Generate a csv lookup from WRS to MGRS:
`python shapefile_to_wrs.py -shapefile ./data/intersecting_wrstiles.shp -wrs_to_mgrs`

## Required Data Files

Shapefiles for the WRS and MGRS grids are required to lookup and convert between the two systems. Download the files from here:

[landsat_downloader_data.zip](https://drive.google.com/file/d/14lqY25kH1sU2kVYO6yR6ASPrDWW3fQ3J/view?usp=sharing)

`grid_files` and `data` directories goes under the main project directory, `test_data` directory goes under the `test` directory in the main project directory.

## Env Vars for USGS EE Auth
Make sure to set USGS_EE_USERNAME and USGS_EE_PASSWORD to the usernamd and password that you use to access USGS EE.
