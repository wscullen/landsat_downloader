# -*- coding: utf-8 -*-
"""usgs_ee_downloader::downloader.py

This module utilizes the USGS JSON API to query and download Landsat 8 and
Sentinel 2 data. More information can be found in the `USGS JSON API DOCS`_.

Example
-------
You create the L8Downloader class with the json config path as the only
argument to the constructor. The config file has ``USGS_USER`` and ``USGS_PASS``
params, which are used for authentication to the JSON api.

``L8Downloader`` is the class of the ``downloader``
module in the  ``landsatdownloader`` package::

    from landsatdownloader.downloader import L8Downloader

    dl_obj = L8Downloader('path/to/json/config/file.json')


Section breaks are created with two blank lines. Section breaks are also
implicitly created anytime a new section starts. Section bodies *may* be
indented:

Notes
-----
    This is an example of an indented section. It's like any other section,
    but the body is indented to help it stand out from surrounding text.

If a section is indented, then a section break is created by
resuming unindented text.

Attributes
----------
module_level_variable1 : int
    Module level variables may be documented in either the ``Attributes``
    section of the module docstring, or in an inline docstring immediately
    following the variable.

    Either form is acceptable, but the two should not be mixed. Choose
    one convention to document module level variables and be consistent
    with it.


.. _USGS JSON API DOCS:
   https://earthexplorer.usgs.gov/inventory/documentation/json-api

"""

import csv
from datetime import datetime
import json
import logging
import math
import os
import requests
import time
import shutil
import sys
import tqdm
from .runningtime import RunningTime
from multiprocessing import Pool
from threading import Thread
from tabulate import tabulate
from osgeo import ogr
import re
import typing
from typing import Dict, Tuple, List, Optional
import queue

from geomet import wkt

from . import utilities

from .transfer_monitor import TransferMonitor
from .utils import TaskStatus

class L8Downloader:

    def __init__(self, path_to_config, username=None, password=None, verbose=False):

        if os.path.exists(path_to_config):
            with open(path_to_config) as f:
                self.config = json.load(f)
        else:
            self.config = None

        self.url_base_string = "https://earthexplorer.usgs.gov/inventory/json/v/1.4.1/"
        self.url_post_string = self.url_base_string + "{}"
        self.url_get_string = self.url_post_string + "?jsonRequest={}"
        self.path_to_config = os.path.dirname(path_to_config)

        self.auth_token = {
            'token': None,
            'last_active': None
        }

        self.max_attempts = 3
        self.initial_delay = 15
        self.api_timeout = 60 * 60

        self.verbose = verbose

        self.username = os.environ.get('USGS_EE_USER')
        self.password = os.environ.get('USGS_EE_PASS')

        if not (bool(self.username) and bool(self.password)):
            print('Missing auth env vars, MISSING USERNAME OR PASSWORD')
            print('Set the appropriate auth values in USGS_EE_USER and USGS_EE_PASS')
            raise('Missing critical auth env vars.')

        # # create logger
        self.logger = logging.getLogger(__name__)
        # self.logger.setLevel(logging.DEBUG)

        # # create console handler and set level to debug
        # ch = logging.StreamHandler()
        # ch.setLevel(logging.DEBUG)

        # # create formatter
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # # add formatter to ch
        # ch.setFormatter(formatter)

        # # add ch to logger
        # self.logger.addHandler(ch)

        # # 'application' code
        # self.logger.debug('debug message')
        # self.logger.info('info message')
        # self.logger.warn('warn message')
        # self.logger.error('error message')
        # self.logger.critical('critical message')

    def authenticate(self):
        """ Read the .json config file to get the user name and password"""

        # self.logger.debug('Attempting a direct login')
        # print(f'config path: {self.path_to_config}')
        auth_file = os.path.join(self.path_to_config, 'authcache.json')
        now = time.time()

        if os.path.isfile(auth_file):
            # self.logger.debug('an authcache file is present')
            # print('found a auth cache file')
            with open(auth_file, 'r') as infile:
                auth_token = json.load(infile)
                time_diff = now - auth_token['last_active']

            if time_diff <= (self.api_timeout):
                # self.logger.debug('time diff is {}, less than 3600 seconds'.format(time_diff))
                # self.logger.debug('auth_token is {}'.format(auth_token))
                # print('auth cache is valid')
                self.auth_token = auth_token
                return self.auth_token
            else:
                # self.logger.debug('time diff is {}, greater than 3600 seconds'.format(time_diff))
                # self.logger.debug('removing out of date authcache and trying to login again')
                os.remove(auth_file)
                # print('auth cache is invalid (timedout)')
                # print('retry authentication')
                self.authenticate()
        else:
            # print('ACTUALLY TRYING TO AUTHENTICATE NOW')
            data = {
                    "username": self.username,
                    "password": self.password,
                    "authType": "EROS",
                    "catalogId": "EE"
            }

            login_url = self.url_post_string.format("login")

            payload = {
                "jsonRequest": json.dumps(data)
            }

            r = requests.post(login_url, payload)
            # print('auth result: ')
            # print(r)


            if r.status_code == 200:
                result = r.json()
                # print(result)
                if result['error'] != '':
                    print(f"Unable to authenticate, error: {result['error']}, errorInfo: {result['errorCode']}")
                    return None
                else:

                    self.auth_token['token'] = result['data']
                    self.auth_token['last_active'] = time.time()

                    with open(auth_file, 'w') as outfile:
                        json.dump(self.auth_token, outfile)

                    return self.auth_token

            else:
                print('There was a problem authenticating, status_code is {}'.format(
                    r.status_code))
                logging.debug(
                    'There was a problem authenticating, status_code = {}'.format(r.status_code))
                return None

    def json_request(self):
        pass

    def auth_attempt(self):
        """Try to login, with exponential backup retry scheme"""

        # self.logger.debug('Attempting to login...')

        attempts=0
        delay=self.initial_delay

        print('trying to auth')
        result=self.authenticate()
        print(f'auth result {result}')

        while attempts < self.max_attempts:
            result=self.authenticate()
            if result:
                return result

            print('Problems authenticating, trying again after delay...')
            time.sleep(delay)
            delay *= 2
            attempts += 1

        # self.logger.debug('Auth not successful, giving up and exiting...')
        return None

    def check_auth(self):
        print('checking auth status')
        now=time.time()

        if self.auth_token['token']:
            time_diff=now - self.auth_token['last_active']

            if time_diff > (self.api_timeout):
                print('tryin to authenticate again because of timeout')
                return self.auth_attempt()

        else:
            print('trying to authenticate because there is no timeout')
            return self.auth_attempt()

    def update_auth_time(self):

        self.auth_token['last_active']=time.time()

        auth_file=os.path.join(self.path_to_config, 'authcache.json')

        with open(auth_file, 'w') as outfile:
            json.dump(self.auth_token, outfile)

    def create_data_search_object_by_polygon(self, dataset_name, polygon, query_dict):
        self.check_auth()

        platform_name = "Unknown"
        # self.logger.debug('trying to search for products')
        poly = ogr.CreateGeometryFromWkt(polygon)
        env = poly.GetEnvelope()
        # print "minX: %d, minY: %d, maxX: %d, maxY: %d" %(env[0],env[2],env[1],env[3])
        lowerleftX = env[0]
        lowerleftY = env[2]

        upperrightX = env[1]
        upperrightY = env[3]

        data =  {
            "datasetName": dataset_name,
            "apiKey": self.auth_token['token'],
            "spatialFilter": {
                "filterType": "mbr",
                    "lowerLeft": {
                "latitude": lowerleftY,
                "longitude": lowerleftX
                    },
                    "upperRight": {
                "latitude": upperrightY,
                "longitude": upperrightX
                    }
            },
            "temporalFilter": {
                        "startDate": query_dict['date_start'].strftime("%Y-%m-%d"),
                        "endDate": query_dict['date_end'].strftime("%Y-%m-%d")
            },
            "includeUnknownCloudCover": False
        }

        if dataset_name == 'LANDSAT_8_C1':
            platform_name = "Landsat-8"

            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {"filterType":"between","fieldId":20522,"firstValue":"0","secondValue":str(query_dict['cloud_percent'])},
                ]
            }
        elif dataset_name == 'SENTINEL_2A':
            platform_name = 'Sentinel-2'
            cloud_maximum_percent = query_dict['cloud_percent']
            converted_cloud_max = math.floor(cloud_maximum_percent / 10) - 1
            print(converted_cloud_max)
            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {"filterType":"between","fieldId":18696,"firstValue":"0","secondValue":str(converted_cloud_max)},
                ]
            }

        return data

    def search_datasets(self, search_term):
        """
            example route /datasets

            example query
            {
                "datasetName": "L8",
            "spatialFilter": {
                "filterType": "mbr",
                "lowerLeft": {
                        "latitude": 44.60847,
                        "longitude": -99.69639
                },
                "upperRight": {
                        "latitude": 44.60847,
                        "longitude": -99.69639
                }
            },
            "temporalFilter": {
                "startDate": "2014-01-01",
                "endDate": "2014-12-01"
            },
                "apiKey": "USERS API KEY"
            }
        """

        self.check_auth()

        # self.logger.debug('trying to search for datasets')

        data={
            "datasetName": search_term,
            "apiKey": self.auth_token['token']
        }

        dataset_url = self.url_post_string.format("datasets")

        payload = {
            "jsonRequest": json.dumps(data)
        }

        r = requests.post(dataset_url, payload)

        result = r.json()

        if r.status_code == 200 and result['errorCode'] == None:
            self.update_auth_time()
            if self.verbose:
                self.list_results(result['data'], ['datasetName',
                                                'datasetFullName',
                                                'startDate',
                                                'endDate',
                                                'supportDownload',
                                                'totalScenes',
                                                'supportBulkDownload',
                                                'bulkDownloadOrderLimit',
                                                'supportCloudCover'
                                                ],
                                                'search_datasets')

        else:
            print('There was a problem getting datasets, status_code = {}, errorCode = {}, error = {}'.format(
                r.status_code, result['errorCode'], result['error']))

    def get_dataset_field_ids(self, dataset_name):
        """
            example route /datasetfields

            example query
            {
                "datasetName": "SENTINEL_2A",
                "apiKey": "USERS API KEY"
            }
        """

        self.check_auth()

        # self.logger.debug('trying to search for datasets')

        data={
            "datasetName": dataset_name,
            "apiKey": self.auth_token['token']
        }

        dataset_url = self.url_post_string.format("datasetfields")

        payload = {
            "jsonRequest": json.dumps(data)
        }

        r = requests.post(dataset_url, payload)

        result = r.json()

        if r.status_code == 200 and result['errorCode'] == None:
            self.update_auth_time()
            if self.verbose:
                self.list_results(result['data'], ['fieldId',
                                                'name',
                                                'fieldLink',
                                                'valueList'
                                                ],
                                                'get_dataset_field_id')

            return result['data']

        else:
            print('There was a problem getting datasets, status_code = {}, errorCode = {}, error = {}'.format(r.status_code, result['errorCode'], result['error']))


    def list_results(self, result, key_list, name_of_api_call, write_to_csv=False):

        def shorten_string(string_to_shorten):
            if len(string_to_shorten) > 35:
                return string_to_shorten[:25] + ' ... ' + string_to_shorten[-10:]
            else:
                return string_to_shorten

        # tabulate([['Alice', 24], ['Bob', 19]], headers=['Name', 'Age'], tablefmt='orgtbl')
        result_list = []
        result_list_full = []

        for r in result:
            row = []
            row_full = []
            for key in key_list:
                row.append(shorten_string(str(r[key])))
                row_full.append(str(r[key]))

            result_list.append(row)
            result_list_full.append(row_full)

        print(tabulate(result_list, headers=key_list, tablefmt='orgtbl'))

        if write_to_csv:
            now = datetime.now()

            file_name = name_of_api_call + now.strftime('%Y%m%d_%H%M') + '.csv'

            with open(file_name, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=',',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(key_list)
                for row in result_list_full:
                    writer.writerow(row)

    def get_total_products(self, data):
        """ Used in conjunction with search for products to get ALL results"""

        self.check_auth()

        dataset_url = self.url_post_string.format("hits")

        payload = {
            "jsonRequest": json.dumps(data)
        }

        r = requests.post(dataset_url, payload, timeout=60)

        result = r.json()
        # print(result)

        if r.status_code == 200 and result['errorCode'] == None:
            return result['data']
        else:
            return -1

    def populate_result_list(self, result, platform_name, dataset_name, detailed=False):
        """ Takes a dictionary of results from the query, returns a standardized
            product_dictionary with correct keys for the metadata
        """

        result_list = []
        if platform_name == 'Landsat-8':

            for r in result['data']['results']:
                product_dict = {}
                product_dict['entity_id'] = r['entityId']

                product_dict['api_source'] = 'usgs_ee'
                product_dict['download_source'] = None
                product_dict['footprint'] = wkt.dumps(r['spatialFootprint'], decimals=5)

                geom = ogr.CreateGeometryFromWkt(product_dict['footprint'])
                # Get Envelope returns a tuple (minX, maxX, minY, maxY)
                env = geom.GetEnvelope()

                def envelope_to_wkt(env_tuple):
                    coord1 = str(env_tuple[0]) + ' ' + str(env_tuple[3])
                    coord2 = str(env_tuple[1]) + ' ' + str(env_tuple[3])
                    coord3 = str(env_tuple[1]) + ' ' + str(env_tuple[2])
                    coord4 = str(env_tuple[0]) + ' ' + str(env_tuple[2])

                    wkt_string = "POLYGON(({}, {}, {}, {}, {}))".format(coord1, coord2, coord3, coord4, coord1)
                    return wkt_string

                product_dict['mbr'] = envelope_to_wkt(env)

                product_dict['dataset_name'] = dataset_name
                product_dict['name'] = r['displayId']
                product_dict['uuid'] = r['entityId']

                product_dict['preview_url'] = r['browseUrl']
                product_dict['manual_product_url'] = r['dataAccessUrl']
                product_dict['manual_download_url'] = r['downloadUrl']
                product_dict['manual_bulkorder_url'] = r['orderUrl']
                product_dict['metadata_url'] = r['metadataUrl']

                # 2017-05-25T15:17:11
                product_dict['last_modified'] = datetime.strptime(r['modifiedDate'], '%Y-%m-%dT%H:%M:%S')
                product_dict['bulk_inprogress'] = r['bulkOrdered']
                product_dict['summary'] = r['summary']

                product_dict['platform_name'] = platform_name

                # TODO: Create a converter that converts PATH/ROW to MGRS and vice Versa
                product_dict['mgrs'] = None

                product_dict['api_source'] = 'usgs_ee'

                result_list.append(product_dict)

        elif platform_name == 'Sentinel-2':
            print('Sentinel2-result dictionary being built')

            for idx, r in enumerate(result['data']['results']):
                product_dict = {}
                product_dict['entity_id'] = r['entityId']
                product_dict['api_source'] = 'usgs_ee'
                product_dict['download_source'] = None
                product_dict['footprint'] = wkt.dumps(r['spatialFootprint'], decimals=5)

                geom = ogr.CreateGeometryFromWkt(product_dict['footprint'])
                # Get Envelope returns a tuple (minX, maxX, minY, maxY)
                env = geom.GetEnvelope()

                def envelope_to_wkt(env_tuple):
                    coord1 = str(env_tuple[0]) + ' ' + str(env_tuple[3])
                    coord2 = str(env_tuple[1]) + ' ' + str(env_tuple[3])
                    coord3 = str(env_tuple[1]) + ' ' + str(env_tuple[2])
                    coord4 = str(env_tuple[0]) + ' ' + str(env_tuple[2])

                    wkt_string = "POLYGON(({}, {}, {}, {}, {}))".format(coord1, coord2, coord3, coord4, coord1)
                    return wkt_string

                product_dict['mbr'] = envelope_to_wkt(env)

                product_dict['dataset_name'] = dataset_name
                product_dict['name'] = r['displayId']
                product_dict['uuid'] = r['entityId']

                product_dict['preview_url'] = r['browseUrl']
                product_dict['manual_product_url'] = r['dataAccessUrl']
                product_dict['manual_download_url'] = r['downloadUrl']
                product_dict['manual_bulkorder_url'] = "n/a"
                product_dict['metadata_url'] = r['metadataUrl'] # TODO: know the path to the metadata file using COPERNICUSAPI, need to formalize it

                # # WHY WAS THIS OMITED?! BECAUSE USGS DOESN't Like being hammered with requests
                # detailed_metadata = self.search_scene_metadata(dataset_name, [product_dict['entity_id']])[0]

                # product_dict['detailed_metadata'] = detailed_metadata
                # 2017-05-25T15:17:11
                # product_dict['last_modified'] = datetime.strptime(r['ingestiondate'], '%Y-%m-%dT%H:%M:%S')
                # product_dict['bulk_inprogress'] = r['bulkOrdered']
                product_dict['summary'] = r['summary']
                # path = next((r['value']
                #                                         for r in detailed_metadata
                #                                             if r['fieldName'] == 'WRS Path'), None)
                # row = next((r['value']
                #                                         for r in detailed_metadata
                #                                             if r['fieldName'] == 'WRS Row'), None)
                product_dict['pathrow'] = "n/a " # TODO: MGRS to PATHROW converter

                # product_dict['land_cloud_percent'] = next((r['value']
                                                        # for r in detailed_metadata
                                                        #     if r['fieldName'] == 'Land Cloud Cover'), None)

                product_dict['platform_name'] = platform_name
                # product_dict['instrument'] = next((r['value']
                #                                         for r in detailed_metadata
                #                                             if r['fieldName'] == 'Sensor Identifier'), None)
                product_dict['api_source'] = 'usgs_ee'

                result_list.append(product_dict)

        if detailed:
            print(result_list)
            # use scene search to get the detailed metadat fields
            result_list = self.fill_detailed_metadata(result_list)


        return result_list

    def search_for_products_by_name(self, dataset_name, product_name_list, query_dict, detailed=False, just_entity_ids=False, write_to_csv=False, call_count=0):
        """
            example route /search

            query_dict needs:
            max cloud
            start date
            end date

            example query
            {
                "datasetName": "LANDSAT_8",
                    "spatialFilter": {
                        "filterType": "mbr",
                        "lowerLeft": {
                    "latitude": 75,
                    "longitude": -135
                        },
                        "upperRight": {
                    "latitude": 90,
                    "longitude": -120
                        }
                    },
                    "temporalFilter": {
                        "startDate": "2006-01-01",
                        "endDate": "2007-12-01"
                    },
                    "additionalCriteria": {
                        "filterType": "or",
                        "childFilters": [
                            {"filterType":"between","fieldId":20515,"firstValue":"0","secondValue":str(query_dict['cloud_percentage'])},
                        ]
                    },
                "maxResults": 3,
                "startingNumber": 1,
                "sortOrder": "ASC",
                "apiKey": "USERS API KEY"
            }
        """
        self.check_auth()
        platform_name = "Unknown"

        data =  {
            "datasetName": dataset_name,
            "apiKey": self.auth_token['token']
        }

        if dataset_name == 'LANDSAT_8_C1':
            platform_name = "Landsat-8"
            # build out product list filter
            child_filter_list = []
            self.logger.debug(product_name_list)

            for product_name in product_name_list:
                filter_dict = {
                     "filterType": "value",
                     "fieldId": 20520,
                     "value": product_name,
                     "operand": "like"
                }
                child_filter_list.append(filter_dict)

            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {"filterType":"between","fieldId":20522,"firstValue":"0","secondValue":str(query_dict['cloud_percent'])},
                    {"filterType":"or",
                        "childFilters": child_filter_list
                    }
                ]
            }

        elif dataset_name == 'SENTINEL_2A':
            platform_name = 'Sentinel-2'
            cloud_maximum_percent = query_dict['cloud_percent']
            converted_cloud_max = math.floor(cloud_maximum_percent / 10) - 1
            self.logger.debug(converted_cloud_max)

            # build out product list filter
            child_filter_list = []
            for product_name in product_name_list:
                # detect if vendor product id or vendor tile id
                # if vendor product id, need convert to vendor tile id
                # we do this because usgs stores the product id with an mgrs from the datastrip(I guess)
                # so if we don't convert it we will miss products because of the non-matching mgrs tile
                # vendor tile id api fieldId = 18699
                # vendor product id api fieldId = 18702
                if product_name[:2] == 'S2':
                    # need to convert it
                    name_parts = product_name.split('_')
                    converted_product_name = f'L1C_{name_parts[5]}'
                    filter_dict = {
                        "filterType": "value",
                        "fieldId": 18702,
                        "value": product_name[:27],
                        "operand": "like"
                    }
                    filter_dict2 = {
                        "filterType": "value",
                        "fieldId": 18699,
                        "value": converted_product_name,
                        "operand": "like"
                    }
                    child_filter_list.append({
                        "filterType": "and",
                        "childFilters": [
                            filter_dict,
                            filter_dict2
                        ]
                    })

                else:
                    # its a vendor tile id already
                    converted_product_name = product_name
                    filter_dict = {
                        "filterType": "value",
                        "fieldId": 18699,
                        "value": converted_product_name,
                        "operand": "like"
                    }
                    child_filter_list.append(filter_dict)

            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {"filterType":"between","fieldId":18696,"firstValue":"0","secondValue":str(converted_cloud_max)},
                    {"filterType":"or",
                        "childFilters": child_filter_list
                    }
                ]
            }

        # print(data)
        dataset_url = self.url_post_string.format("search")
        all_results = []

        # total_num = self.get_total_products(data)
        # if total_num == -1:
        #     print('something went wrong, got no results')
        #     return []

        # data['maxResults'] = total_num
        data['maxResults'] = 5000
        # print(total_num)
        payload = {
            "jsonRequest": json.dumps(data)
        }

        time.sleep(0.25)
        r = requests.post(dataset_url, payload, timeout=300)

        if r.status_code == 200:
            result = r.json()

            if result['errorCode'] == None:
                self.update_auth_time()
                if self.verbose:
                    self.list_results(result['data']['results'],
                                                ['acquisitionDate',
                                                'spatialFootprint',
                                                'browseUrl',
                                                'downloadUrl',
                                                'entityId',
                                                'metadataUrl',
                                                'summary',
                                                'bulkOrdered',
                                                'ordered'
                                                ],
                                                'search_for_products', write_to_csv=write_to_csv)

                result_list = self.populate_result_list(result, platform_name, dataset_name, detailed=detailed)

                if just_entity_ids:
                    return [r['entity_id'] for r in result_list]
                else:
                    return result_list

            elif result['errorCode'] == 'RATE_LIMIT':
                print('API access is denied because of a RATE LIMIT issue. Waiting for 5 mins and calling again.')
                print(f'Current retry count at {call_count}')

                if call_count > self.max_attempts:
                    print('Max retries exceeded. Giving up on current task')
                    return []

                time.sleep(60 * 5)
                call_count += 1

                self.search_for_products_by_name(dataset_name, product_name_list, query_dict, call_count=call_count)

        else:
            print('There was a problem getting products, status_code = {}, errorCode = {}, error = {}'.format(r.status_code, result['errorCode'], result['error']))
            return []

    def search_for_products(self, dataset_name, polygon, query_dict, detailed=False, just_entity_ids=False, write_to_csv=False):
        """
            example route /search

            query_dict needs:
            max cloud
            start date
            end date

            example query
            {
                "datasetName": "LANDSAT_8",
                    "spatialFilter": {
                        "filterType": "mbr",
                        "lowerLeft": {
                    "latitude": 75,
                    "longitude": -135
                        },
                        "upperRight": {
                    "latitude": 90,
                    "longitude": -120
                        }
                    },
                    "temporalFilter": {
                        "startDate": "2006-01-01",
                        "endDate": "2007-12-01"
                    },
                    "additionalCriteria": {
                        "filterType": "or",
                        "childFilters": [
                            {"filterType":"between","fieldId":20515,"firstValue":"0","secondValue":str(query_dict['cloud_percentage'])},
                        ]
                    },
                "maxResults": 3,
                "startingNumber": 1,
                "sortOrder": "ASC",
                "apiKey": "USERS API KEY"
            }
        """

        self.check_auth()
        platform_name = "Unknown"
        poly = ogr.CreateGeometryFromWkt(polygon)
        env = poly.GetEnvelope()
        # print "minX: %d, minY: %d, maxX: %d, maxY: %d" %(env[0],env[2],env[1],env[3])
        lowerleftX = env[0]
        lowerleftY = env[2]

        upperrightX = env[1]
        upperrightY = env[3]

        data =  {
            "datasetName": dataset_name,
            "apiKey": self.auth_token['token'],
            "spatialFilter": {
                "filterType": "mbr",
                    "lowerLeft": {
                "latitude": lowerleftY,
                "longitude": lowerleftX
                    },
                    "upperRight": {
                "latitude": upperrightY,
                "longitude": upperrightX
                    }
            },
            "temporalFilter": {
                        "startDate": query_dict['date_start'].strftime("%Y-%m-%d"),
                        "endDate": query_dict['date_end'].strftime("%Y-%m-%d")
            }
        }

        if dataset_name == 'LANDSAT_8_C1':
            platform_name = "Landsat-8"

            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {"filterType":"between","fieldId":20522,"firstValue":"0","secondValue":str(query_dict['cloud_percent'])},
                ]
            }
        elif dataset_name == 'SENTINEL_2A':
            platform_name = 'Sentinel-2'
            cloud_maximum_percent = query_dict['cloud_percent']
            converted_cloud_max = math.floor(cloud_maximum_percent / 10) - 1
            print(converted_cloud_max)
            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {"filterType":"between","fieldId":18696,"firstValue":"0","secondValue":str(converted_cloud_max)},
                ]
            }

        dataset_url = self.url_post_string.format("search")
        all_results = []

        # total_num = self.get_total_products(data)
        # if total_num == -1:
        #     print('something went wrong, got no results')
        #     return []

        # data['maxResults'] = total_num
        data['maxResults'] = 5000
        # print(total_num)
        payload = {
            "jsonRequest": json.dumps(data)
        }
        time.sleep(0.25)
        r = requests.post(dataset_url, payload, timeout=240)
        self.logger.debug(r)
        result = r.json()

        if r.status_code == 200 and result['errorCode'] == None:
            self.update_auth_time()
            if self.verbose:
                self.list_results(result['data']['results'],
                                            ['acquisitionDate',
                                            'spatialFootprint',
                                            'browseUrl',
                                            'downloadUrl',
                                            'entityId',
                                            'metadataUrl',
                                            'summary',
                                            'bulkOrdered',
                                            'ordered'
                                            ],
                                            'search_for_products', write_to_csv=write_to_csv)

            result_list = self.populate_result_list(result, platform_name, dataset_name, detailed=detailed)

            if just_entity_ids:
                return [r['entity_id'] for r in result_list]
            else:
                return result_list
        else:
            print('There was a problem getting products, status_code = {}, errorCode = {}, error = {}'.format(r.status_code, result['errorCode'], result['error']))

    def search_for_products_by_tile(self, dataset_name, tile_list, query_dict, just_entity_ids=False, write_to_csv=False, detailed=False):
        """
        Same as search_for_products, but using a direct tile list

        See search_for_products for example on a USGS EE query formation.
        """

        # Sentinel2 fieldId for tile number (TXXXXX)
        # 18701

        self.check_auth()
        platform_name = "Unknown"

        data =  {
            "datasetName": dataset_name,
            "apiKey": self.auth_token['token'],
            "temporalFilter": {
                        "startDate": query_dict['date_start'].strftime("%Y-%m-%d"),
                        "endDate": query_dict['date_end'].strftime("%Y-%m-%d")
            }
        }

        if dataset_name == 'LANDSAT_8_C1':
            platform_name = "Landsat-8"
            cloud_maximum_percent = query_dict['cloud_percent']
            converted_cloud_max = int(math.ceil(cloud_maximum_percent / 10.0)) * 10
            print(converted_cloud_max)
            # build out product list filter
            child_filter_list = []
            for pathrow in tile_list:
                print(pathrow[:3])
                print(pathrow[3:])
                filter_dict_path = {
                    "filterType": "value",
                    "fieldId": 20514,
                    "value": ' ' + pathrow[:3],
                    "operand": "="
                }

                filter_dict_row = {
                    "filterType": "value",
                    "fieldId": 20516,
                    "value": ' ' + pathrow[3:],
                    "operand": "="
                }

                filter_pathrow = {
                    "filterType": "and",
                    "childFilters": [
                        filter_dict_path,
                        filter_dict_row
                    ]
                }

                child_filter_list.append(filter_pathrow)


            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {"filterType":"between","fieldId":20522,"firstValue":"0","secondValue":str(converted_cloud_max)},
                    {"filterType": "or",
                        "childFilters": child_filter_list
                    }
                ]
            }
                # "filterType": "and",
                # "childFilters": [

                            # },
                            # {
                            #     "filterType": "value",
                            #     "fieldId": 20516,
                            #     "value": " 025",
                            #     "operand": "like"
                            # }

                        #     filter_pathrow = {
                        #         "filterType": "and",
                        #         "childFilters": [
                        #             filter_dict_path,
                        #             filter_dict_row
                        #         ]
                        #     }

                        #     child_filter_list.append(filter_pathrow)
                    # #
            #     ]
            # }
            # TODO: Fix this later
            # data["additionalCriteria"] = {
            #     "filterType": "and",
            #     "childFilters": [
            #         {"filterType": "between","fieldId":20522,"firstValue":"0","secondValue":str(query_dict['cloud_percent'])},
            #         {"filterType": "or", "fieldId": }
            #     ]
            # }
        elif dataset_name == 'SENTINEL_2A':
            platform_name = 'Sentinel-2'
            cloud_maximum_percent = query_dict['cloud_percent']
            converted_cloud_max = math.floor(cloud_maximum_percent / 10) - 1
            print(converted_cloud_max)
            # build out product list filter
            child_filter_list = []
            for gzd_100km in tile_list:
                filter_dict = {
                    "filterType": "value",
                    "fieldId": 18701,
                    "value": gzd_100km,
                    "operand": "like"
                }

                child_filter_list.append(filter_dict)

            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {"filterType":"between","fieldId":18696,"firstValue":"0","secondValue":str(converted_cloud_max)},
                    {"filterType":"or",
                        "childFilters": child_filter_list
                    }
                ]
            }

        dataset_url = self.url_post_string.format("search")
        all_results = []

        data['maxResults'] = 10000
        payload = {
            "jsonRequest": json.dumps(data)
        }

        print(payload)
        time.sleep(0.25)
        r = requests.post(dataset_url, payload, timeout=240)
        self.logger.debug(r)
        result = r.json()

        if r.status_code == 200 and result['errorCode'] == None:
            self.update_auth_time()
            if self.verbose:
                self.list_results(result['data']['results'],
                                            ['acquisitionDate',
                                            'spatialFootprint',
                                            'browseUrl',
                                            'downloadUrl',
                                            'entityId',
                                            'metadataUrl',
                                            'summary',
                                            'bulkOrdered',
                                            'ordered'
                                            ],
                                            'search_for_products', write_to_csv=write_to_csv)

            print(len(result))
            print(result)

            result_list = self.populate_result_list(result, platform_name, dataset_name, detailed=detailed)

            if just_entity_ids:
                return [r['entity_id'] for r in result_list]
            else:
                return result_list
        else:
            print('There was a problem getting products, status_code = {}, errorCode = {}, error = {}'.format(r.status_code, result['errorCode'], result['error']))


    def search_for_products_polygon_to_tiles(self,
                                             dataset_name: str,
                                             polygon: str,
                                             query_dict: Dict,
                                             detailed: bool = False,
                                             just_entity_ids: bool = False,
                                             write_to_csv: bool = False) -> Tuple[List[Dict], Optional[Dict]]:
        """Same as search_for_products, with polygon derived tiles instead.

        See search_for_products for example on a USGS EE query formation.

        1. Use utilities module to determine actual MGRS or WRS2 tiles that
           the polygon intersects with.
        2. Form the query using those as additional criteria rather than using
           the polygon as a spatial query directly
        3. Otherwise the same as search_for_products

        Parameters
        ----------
        dataset_name
            Dataset identifier used by USGS EE (LANDSAT_C1, SENTINEL_2A)
        polygon
            WKT single polygon representing the footprint of the AoI
        query_dict
            Holds key values for searching including:
            date_start: date ('YYYY-MM-DD') start temporal range
            date_end: date ('YYYY-MM-DD') end temporal range
            footprint: str WKT single polygon of the spatial range
            cloud_percent: int Max percent of clouds allowed in search
        detailed
            Whether or not the detailed metadata values will be queried and
            added to each product of the result list.
            Note
            ----
            If detailed is set to False, the original raw result dictionary is
            returned as the second member of the return tuple. This allows you
            to look up detailed metadata using the func populate_result_list
            at a later time. If detailed is set to True, the second member of
            the return tuple is None.
        just_entity_ids
            If true, the result list returned is simple a list of entity ids.
        write_to_csv
            If true, the search result dicts for each product are written out to
            an equivalent CSV file.
        """

        # Sentinel2 fieldId for tile number (TXXXXX)
        # 18701

        self.check_auth()
        platform_name = "Unknown"

        # 1. parse polygon into a list of MGRS gzd or WRS2 pathrow
        gzd_list = utilities.find_mgrs_intersection_large(polygon)

        gzd_list_100km = utilities.find_mgrs_intersection_100km(polygon,
                                                                gzd_list)

        print(gzd_list_100km)

        poly = ogr.CreateGeometryFromWkt(polygon)
        env = poly.GetEnvelope()
        # print "minX: %d, minY: %d, maxX: %d, maxY: %d" %(env[0],env[2],env[1],env[3])
        lowerleftX = env[0]
        lowerleftY = env[2]

        upperrightX = env[1]
        upperrightY = env[3]

        data =  {
            "datasetName": dataset_name,
            "apiKey": self.auth_token['token'],
            "temporalFilter": {
                        "startDate": query_dict['date_start'].strftime("%Y-%m-%d"),
                        "endDate": query_dict['date_end'].strftime("%Y-%m-%d")
            },
            "spatialFilter": {
                "filterType": "mbr",
                    "lowerLeft": {
                "latitude": lowerleftY,
                "longitude": lowerleftX
                    },
                    "upperRight": {
                "latitude": upperrightY,
                "longitude": upperrightX
                    }
            },
        }

        if dataset_name == 'LANDSAT_8_C1':
            platform_name = "Landsat-8"

            # TODO: Fix this later
            # data["additionalCriteria"] = {
            #     "filterType": "and",
            #     "childFilters": [
            #         {"filterType": "between","fieldId":20522,"firstValue":"0","secondValue":str(query_dict['cloud_percent'])},
            #         {"filterType": "or", "fieldId": }
            #     ]
            # }
        elif dataset_name == 'SENTINEL_2A':
            platform_name = 'Sentinel-2'
            cloud_maximum_percent = query_dict['cloud_percent']
            converted_cloud_max = math.floor(cloud_maximum_percent / 10) - 1
            print(converted_cloud_max)

            # # build out product list filter
            # child_filter_list = []
            # for gzd_100km in gzd_list_100km:
            #     filter_dict = {
            #         "filterType": "value",
            #         "fieldId": 18701,
            #         "value": gzd_100km,
            #         "operand": "like"
            #     }

            #     child_filter_list.append(filter_dict)

            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {"filterType":"between","fieldId":18696,"firstValue":"0","secondValue":str(converted_cloud_max)},
                    # {"filterType":"or",
                    #     "childFilters": child_filter_list
                    # }
                ]
            }

        dataset_url = self.url_post_string.format("search")
        all_results = []

        # total_num = self.get_total_products(data)
        # if total_num == -1:
        #     print('something went wrong, got no results')
        #     return []

        # data['maxResults'] = total_num
        data['maxResults'] = 5000
        # print(total_num)
        payload = {
            "jsonRequest": json.dumps(data)
        }
        time.sleep(0.25)
        r = requests.post(dataset_url, payload, timeout=240)
        self.logger.debug(r)
        result = r.json()

        if r.status_code == 200 and result['errorCode'] == None:
            self.update_auth_time()
            if self.verbose:
                self.list_results(result['data']['results'],
                                            ['acquisitionDate',
                                            'spatialFootprint',
                                            'browseUrl',
                                            'downloadUrl',
                                            'entityId',
                                            'metadataUrl',
                                            'summary',
                                            'bulkOrdered',
                                            'ordered'
                                            ],
                                            'search_for_products', write_to_csv=write_to_csv)

            print(len(result['data']['results']))
            # Use to save out intermediate results for testing purposes
            # with open('raw_alberta_aug2018_results.json', 'w') as outfile:
            #     json.dump(result, outfile)

            temp_results = utilities.filter_by_footprint(polygon,
                                                         result['data']['results'],
                                                         dataset_name)

            # real_result_list = []

            # for product in temp_results:
            #     mgrs_id = product['displayId'].split('_')[1][1:]
            #     # print(mgrs_id)
            #     if mgrs_id in gzd_list_100km:

            #         real_result_list.append(product)

            result['data']['results'] = temp_results

            result_list = self.populate_result_list(result, platform_name, dataset_name, detailed=detailed)

            if just_entity_ids:
                return [r['entity_id'] for r in result_list]
            else:
                return result_list
        else:
            print('There was a problem getting products, status_code = {}, errorCode = {}, error = {}'.format(r.status_code, result['errorCode'], result['error']))

    def fill_detailed_metadata(self, product_list):
        """
        Helper function to get all metadata for a product list.

        Uses search_scene_metadata to find the additional metadata.
        """

        logging.info('populating detailed metadata for each entity')
        result_list = []
        print(product_list)
        detailed_metadata_list = self.search_scene_metadata(product_list[0]['dataset_name'], [r['entity_id'] for r in product_list])
        print(detailed_metadata_list)

        for r in product_list:
            if r['platform_name'] == 'Landsat-8':
                product_dict = dict(r)
                # start time = '2017:135:18:29:18.4577340'
                # datetime.strptime('Jun 1 2005  1:33PM', '%Y:%j:%H:%M:%S.%f')
                # Iterate through metadata list, find the field, convert to datetime obj,
                # select the first one
                # print(r)


                detailed_metadata = [md for md in detailed_metadata_list if md['entityId'] == r['entity_id']][0]['metadataFields']
                product_dict['detailed_metadata'] = detailed_metadata

                utm_zone = [r['value'] for r in product_dict['detailed_metadata'] if r['fieldName'] == 'UTM Zone'][0]
                center_latitude = [r['value'] for r in product_dict['detailed_metadata'] if r['fieldName'] == 'Center Latitude'][0]
                north_south = center_latitude[-1]
                proj_start = '326' if north_south == 'N' else '327'
                product_dict['epsg_code'] = proj_start + utm_zone

                product_dict['vendor_name'] = r['name']
                product_dict['acquisition_start'] = next((datetime.strptime(field['value'][:-2], '%Y:%j:%H:%M:%S.%f')
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'Start Time'), None)

                product_dict['acquisition_end'] = next((datetime.strptime(field['value'][:-2], '%Y:%j:%H:%M:%S.%f')
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'Stop Time'), None)

                path = next((field['value']
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'WRS Path'), None)
                row = next((field['value']
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'WRS Row'), None)
                product_dict['pathrow'] = path + row

                product_dict['land_cloud_percent'] = next((field['value']
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'Land Cloud Cover'), None)

                product_dict['cloud_percent'] = next((field['value']
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'Scene Cloud Cover'), None)

                product_dict['instrument'] = next((field['value']
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'Sensor Identifier'), None)

                product_dict['sat_name'] = 'LANDSAT8'


                result_list.append(product_dict)

            elif r['platform_name'] == 'Sentinel-2':
                logging.info('Sentinel2 detailed metadata being populated')

                product_dict = dict(r) # copy the plain product dict without detailed metadata

                detailed_metadata = [md for md in detailed_metadata_list if md['entityId'] == r['entity_id']][0]['metadataFields']
                product_dict['detailed_metadata'] = detailed_metadata

                product_dict['epsg_code'] = [r['value'] for r in product_dict['detailed_metadata'] if r['fieldName'] == 'EPSG Code'][0]

                # start time = '2017:135:18:29:18.4577340'
                # datetime.strptime('Jun 1 2005  1:33PM', '%Y:%j:%H:%M:%S.%f')
                # Iterate through metadata list, find the field, convert to datetime obj,
                # select the first one
                # Acquisition Start Date', 'descriptionLink': 'https://lta.cr.usgs.gov/Sentinel2#acqu
                # isition_date_start', 'value': '2018-05-02T18:40:47.049Z'},
                product_dict['acquisition_start'] = next((datetime.strptime(field['value'][:-2], '%Y-%m-%dT%H:%M:%S.%f')
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'Acquisition Start Date'), None)

                product_dict['acquisition_end'] = next((datetime.strptime(field['value'][:-2], '%Y-%m-%dT%H:%M:%S.%f')
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'Acquisition End Date'), None)

                product_dict['cloud_percent'] = next((field['value']
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'Cloud Cover'), None)

                # TODO: Create a converter that converts PATH/ROW to MGRS and vice Versa
                product_dict['mgrs'] = next((field['value']
                                                        for field in detailed_metadata
                                                            if field['fieldName'] == 'Tile Number'), None)
                product_dict['api_source'] = 'usgs_ee'

                product_dict['sat_name'] = 'Sentinel2'

                # Have to a bunch of conversions here becuase the usgs product vendor id does not match the MGRS
                # of the other properties
                summary_string = product_dict['summary'].split(',')[0][11:]

                if summary_string[:7] == 'S2A_OPER':
                    for r in product_dict['detailed_metadata']:
                         if r['fieldName'] == 'Vendor Product ID':
                             r['value'] = summary_string
                else:
                    vendor_name = [r['value'] for r in product_dict['detailed_metadata'] if r['fieldName'] == 'Vendor Product ID'][0]
                    temp_arr = vendor_name.split('_')
                    temp_arr[5] = product_dict['mgrs']
                    correct_product_name = '_'.join(temp_arr)

                product_dict['vendor_name'] = correct_product_name
                print(f"vendor name: {product_dict['vendor_name']}")
                result_list.append(product_dict)

        return result_list


    def search_scene_metadata(self, dataset_name, entity_id_list, write_to_csv=False):
        """
        /metadata

        {
                "apiKey": "USERS API KEY",
                "datasetName": "LANDSAT_8",
                "entityIds": ["LC80130292014100LGN00"]
        }

        """
        self.check_auth()

        # self.logger.debug('trying to search for metadata fields in the {} dataset'.format(dataset_name))

        data =  {
            "datasetName": dataset_name,
            "apiKey": self.auth_token['token'],
            "entityIds": entity_id_list
        }

        dataset_url = self.url_post_string.format("metadata")

        payload = {
            "jsonRequest": json.dumps(data)
        }

        r = requests.post(dataset_url, payload)


        result = r.json()
        metadata_list = []
        if r.status_code == 200:
            self.update_auth_time()

            if result['errorCode'] == None:
                if self.verbose:
                    self.list_results(result['data'], result['data'][0].keys(),
                                                'search_scene_metadata', write_to_csv=write_to_csv)

                for r in result['data']:
                    metadata_list.append(r)

                return metadata_list
        else:
            print('There was a problem getting datasets, status_code = {}, errorCode = {}, error = {}'.format(r.status_code, result['errorCode'], result['error']))

    def search_download_options(self, dataset_name, entity_id_list, write_to_csv=False):
        """
        /downloadoptions
        {
            "datasetName": "LANDSAT_8",
            "apiKey": "USERS API KEY",
            "entityIds": ["LC80130292014100LGN00"]
        }
        """

        self.check_auth()

        # self.logger.debug('trying to search for fields in the {} dataset'.format(dataset_name))

        data =  {
            "datasetName": dataset_name,
            "apiKey": self.auth_token['token'],
            "entityIds": entity_id_list
        }

        dataset_url = self.url_post_string.format("downloadoptions")

        payload = {
            "jsonRequest": json.dumps(data)
        }

        r = requests.post(dataset_url, payload)

        result = r.json()

        if r.status_code == 200 and result['errorCode'] == None:
            self.update_auth_time()
            if self.verbose:
                self.list_results(result['data'],
                                  ['downloadOptions', 'entityId'],
                                  'search_dataset_fields',
                                  write_to_csv=write_to_csv)

            product_set = set()

            for product in result['data']:
                for download_product in product['downloadOptions']:
                    product_set.add(download_product['downloadCode'])

            return list(product_set)

        else:
            print('There was a problem getting datasets, status_code = {}, errorCode = {}, error = {}'.format(r.status_code, result['errorCode'], result['error']))

    def get_download_urls(self, dataset_name, entity_id_list, product_list, auth_token=None):
        """
        /download
        {
            "datasetName": "GLS_ALL",
            "apiKey": "USERS API KEY",
            "entityIds": ["P046R034_1X19720725"],
            "products": ["FR_REFL", "STANDARD"]
        }
        """

        if auth_token:
            token = auth_token
            self.logger.debug('external auth token (multiprocessing)')
        else:
            self.logger.debug('internal auth token (single process)')
            self.check_auth()
            token = self.auth_token

        # self.logger.debug('trying to search for fields in the {} dataset'.format(dataset_name))

        data =  {
            "datasetName": dataset_name,
            "apiKey": token['token'],
            "entityIds": entity_id_list,
            "products": product_list
        }

        dataset_url = self.url_post_string.format("download")

        payload = {
            "jsonRequest": json.dumps(data)
        }

        r = requests.post(dataset_url, payload)

        result = r.json()

        print(result)
        print(r.status_code)

        if r.status_code == 200 and result['errorCode'] == None:
            if not auth_token:
                self.update_auth_time()

            if self.verbose:
                self.list_results(result['data'], [
                                                'entityId',
                                                'product',
                                                'url'
                                                ],
                                                'get_download_urls', write_to_csv=True)

            return [{'url': r['url'], 'entity_id': r['entityId'], 'product': r['product']} for r in result['data']]

        else:
            print('There was a problem getting download urls, status_code = {}, errorCode = {}, error = {}'.format(r.status_code, result['errorCode'], result['error']))

    def download_file(self, filename, url, id=0):
        # NOTE the stream=True parameter

        # self.check_auth() Since auth is baked into the url passed back from get
        # download url, the auth check is unnecessary
        print('Trying to download the file')
        r = requests.get(url, stream=True, timeout=60*60)
        print(r.status_code)
        if not os.path.isfile(filename):
            try:

                transfer = TransferMonitor(filename, id)
                with open(filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1000000):
                        f.write(chunk)
                transfer.finish()

            except BaseException as e:
                return TaskStatus(False, 'An exception occured while trying to download.', e)
            else:
                return TaskStatus(True, 'Download successful', filename)
        else:
            return TaskStatus(False, 'Requested file to download already exists.', filename)

    def search_dataset_fields(self, dataset_name):
        """
        /datasetfields
        {
        "apiKey": "USERS API KEY",
        "datasetName": "LANDSAT_8"
        }
        """

        self.check_auth()

        # self.logger.debug('trying to search for fields in the {} dataset'.format(dataset_name))

        data =  {
            "datasetName": dataset_name,
            "apiKey": self.auth_token['token']
        }

        dataset_url = self.url_post_string.format("datasetfields")

        payload = {
            "jsonRequest": json.dumps(data)
        }

        r = requests.post(dataset_url, payload)

        result = r.json()

        if r.status_code == 200 and result['errorCode'] == None:
            self.update_auth_time()
            if self.verbose:
                self.list_results(result['data'],
                                ['fieldId',
                                'name',
                                'fieldLink',
                                'valueList'],
                                'search_dataset_fields', write_to_csv=True)
            return result
        else:
            print('There was a problem getting datasets, status_code = {}, errorCode = {}, error = {}'.format(r.status_code, result['errorCode'], result['error']))

    def download_products(self, product_list, product_type, date_string, auth_token=None):
        """ Iterates over list of products, starts download_task for each

        """

        print('Starting download tasks... this may take a while!')
        path = os.path.join(
                            date_string + 'jobstatus.json')

        # Create a pool of 2 workers, iterate over the list of products and
        # start the tasks as workers become available
        with Pool(processes=4) as pool:

            job_runtime = RunningTime()

            async_processes = []

            # Start the jobs, as they are completed save the returned status in
            # the job status list
            for index, product in enumerate(product_list):

                print('starting a task')
                async_started = pool.apply_async(self.download_product,
                                                (product,
                                                product_type),
                                                    {
                                                        "auth_token": auth_token
                                                    }
                                                )

                # self.logger.debug('Starting task {}'.format(index))
                async_processes.append(async_started)

                time.sleep(5)

            result_list = []
            # we wrap layer in progressbar generator to gain access to
            # a progress bar display
            task_iter = tqdm.tqdm(async_processes)
            # for i, p in enumerate(task_iter) does not work

            for p in task_iter:
                # logger.debug('Getting task {}'.format(i))
                # self.logger.debug('Waiting for results from each process...')
                # as results come in we save the json file status
                result = None

                try:
                    # self.logger.debug('Trying to fetch result of task...')
                    result = p.get(timeout=60*60)
                except Exception as e:
                    self.logger.debug(e)
                    # self.logger.debug('Something went wrong')

                else:
                    # self.logger.debug('Task was successful')
                    result_list.append(result)


            # self.logger.debug('Trying to close the pool and join the tasks...')
            pool.close()
            pool.join()

            # self.logger.debug('All download, correction, and conversion tasks completed')

            # TODO: filter the dictlist by products with download and corrected
            # status of succdownload_productess, so you don't try to convert bad data

            # self.logger.debug('Writing final job status...')
            tqdm.tqdm.write('All tasks completed!')

    def download_product(self, product_dict, product_type, directory=None, id=0, auth_token=None):
        """
            Get the download url for a given entity_id and product type

            Once the url is returned, download the file so that it is dequeued on the usgs servers

            the download url is temporary and should be downloaded immediately

        """
        if not auth_token:
            self.check_auth()
            auth_token = self.auth_token

        print('Downloading single product with L8Downloader')
        file_name = ''

        if product_dict['platform_name'] == 'Landsat-8':
            if product_type in ['FR_BUND']:
                file_name = product_dict['name'] + '_{}.zip'.format(product_type)
            elif product_type in [ 'FR_THERM', 'FR_QB', 'FR_REFL']:
                file_name = product_dict['name'] + '_{}.jpg'.format(product_type)
            elif product_type in ['STANDARD']:
                file_name = product_dict['name'] + '.tar.gz'

        elif product_dict['platform_name'] == 'Sentinel-2':

            if product_type in ['STANDARD']:
                file_name = product_dict['name'] + '.zip'
            elif product_type in ['FRB']:
                file_name = product_dict['name'] + '_{}.jpg'.format(product_type)

        if directory:
            file_name = os.path.join(directory, file_name)

        print(product_dict)
        print(product_type)
        print(directory)
        print(auth_token)
        print(file_name)

        print(os.path.isfile(file_name))


        if not os.path.isfile(file_name):
            download_url = self.get_download_urls(product_dict['dataset_name'],
                                                 [product_dict['entity_id']],
                                                 [product_type],
                                                 auth_token=auth_token)

            print(download_url)

            if download_url:
                print('found download url okay, downloading file...')
                # download_file returns a TaskStatus named tuple
                result = self.download_file(file_name, download_url[0]['url'], id)

                if result.status and os.path.isfile(file_name):
                    return result
                else:
                    return TaskStatus(False, 'The download file cannot be found', None)
            else:
                # Return a TaskStatus named tuple
                #return (False, 'Download URL could not be determined', None)
                return TaskStatus(False, 'Download URL could not be determined', None)
        else:
            # Return a TaskStatus named tuple
            #return (True, 'File already exists.', file_name)
            return TaskStatus(True, 'Product to be downloaded already exists.', file_name)





    # ---------------------------------------------------------------------------
    # -- Bulk Downloader API functions ------------------------------------------

    def bulk_submit_order(self, product_list):
        """
        ["LC08_L1TP_008027_20170501_20170515_01_T1", "LC08_L1TP_008028_20170501_20170515_01_T1", "LC08_L1TP_009027_20170508_20170515_01_T2"]
        ["L1C_T20TLS_A009746_20170504T151653", "L1C_T20TLR_A009746_20170504T151653", "L1C_T20TMS_A009746_20170504T151653"]

        datapayload = {
            "format": "GTIFF",
            "note": "Shaun's Order!!",
            "SENTINEL_2A": {
                "inputs": ['2483642'],
            }
        }
        """

        username = self.username
        password = self.password

        inputs = [prod['name'] for prod in product_list]

        date_note = datetime.now().strftime('%Y%m%d-%H:%M-ACGEO-{}'.format(str(len(inputs))))

        datapayload = {
            "format": "GTIFF",
            "note": date_note,
            "olitirs8_collection": {
                "inputs": inputs,
                "products": [
                    "sr"
                ]
            }
        }

        r = requests.post(url='https://espa.cr.usgs.gov/api/v1/order', json=datapayload, auth=(username, password))

        response = r.json()

        self.logger.debug(r.status_code)

        if r.status_code in [200, 201]:
            return response['orderid']
        else:
            return False

    def batch_submit_order(self, product_list, batch_size=10):
        """ Break up the bulk order so that results are received quicker.

        """
        batch_list = []

        # Parse the entire list into a series of slices the size of the batch
        for idx in range(0, (len(product_list) // 10) + 1):
            batch_list.append(product_list[idx * batch_size : (idx * batch_size) + batch_size ])

        self.logger.debug(len(product_list))

        result = []

        for batch in batch_list:
            res = self.bulk_submit_order(batch)
            result.append(res)
            # Avoid USGS rate limiting
            time.sleep(30)

        return result

    def check_current_orders(self):
        """ See if there are outstanding orders that the user should download
            Useful to do before the user starts another order
        """
        username = self.username
        password = self.password
        r = requests.get(url='https://espa.cr.usgs.gov/api/v1/list-orders', auth=(username, password))
        # print(r)
        # print(r.headers)


        if r.status_code == 200:
            response = r.json()
            if len(response) != 0:
                products_list = []
                for order_id in response:
                    order_dict = self.get_order_entity_ids(order_id)
                    if order_dict['status'] in ['ordered', 'completed']:
                        print("ORDER ID: {}, Info: {}".format(order_id, order_dict))
                        products_list.append(order_dict)

                return products_list
        else:
            print('There was a problem connecting to the USGS API, please try again later.')
            print(r.status_code)
            return False

    def check_order_status(self, order_id):
        username = self.username
        password = self.password
        r = requests.get(url='https://espa.cr.usgs.gov/api/v1/order-status/{}'.format(order_id), auth=(username, password))

        response = r.json()
        self.logger.debug(r.status_code)
        if r.status_code == 200:
            if response['status'] == 'complete':
                return True
            else:
                return False
        else:
            return False


    def get_order_entity_ids(self, order_id):
        username = self.username
        password = self.password
        r = requests.get(url='https://espa.cr.usgs.gov/api/v1/order/{}'.format(order_id), auth=(username, password))
        response = r.json()

        if r.status_code in [200, 201]:
            order_dict = {
                "status" :response["status"],
                "inputs_list": response["product_opts"]["olitirs8_collection"]["inputs"],
                "order_date": response["order_date"],
                "note": response["note"],
                "order_id": order_id
            }

            return order_dict
        else:
            return False

    def cancel_order(self, order_id):
        username = self.username
        password = self.password


        data_payload = {
            "orderid": order_id,
            "status": "cancelled"
        }

        r = requests.put(url='https://espa.cr.usgs.gov/api/v1/order', json=data_payload, auth=(username, password))


        response = r.json()

        self.logger.debug(r.status_code)
        self.logger.debug(response)

        if r.status_code in [200, 201, 202]:

            return True
        else:
            return False


    def download_order(self, order_id, directory=None, verify=False):
        username = self.username
        password = self.password
        r = requests.get(url='https://espa.cr.usgs.gov/api/v1/item-status/{}'.format(order_id), auth=(username, password))

        response = r.json()

        if r.status_code in [200, 201]:
            # fill in download code here for each item
            item_list = response[order_id]
            final_list = []

            for item in item_list:
                download_url = item['product_dload_url']

                r = requests.get(download_url, stream=True)

                if directory:
                    file_name = os.path.split(item['product_dload_url'])[1]
                    final_list.append(file_name)
                    file_path = os.path.join(directory, file_name)
                else:
                    file_name = os.path.split(item['product_dload_url'])[1]
                    final_list.append(file_name)
                    file_path = file_name

                success = False
                if not os.path.isfile(file_path):
                    print(f'trying to download {file_path}')
                    while not success:
                        try:
                            with open(file_path, 'wb') as f:
                                for chunk in r.iter_content(chunk_size=1024):
                                    if chunk: # filter out keep-alive new chunks
                                        f.write(chunk)
                        except Exception as e:
                            print(f'bad thing happened, usgs is not cooperating {e}')
                            os.remove(file_path)
                            time.sleep(15)
                        else:
                            success = True
                            time.sleep(10)

                else:
                    print('file to be downloaded already exists')

            return (True, 'Downloading finished', final_list)
        else:
            return (False, 'Bad return from the server')

    def check_if_products_exist(self, name_list, directory, type_of_product):
        # Create a copy of the list
        not_exist_set = [x for x in name_list]

        self.logger.debug(not_exist_set)
        self.logger.debug(name_list)

        # For each entity name, see if an equiv product already exists
        for product_name in name_list:
            part_array = product_name.split('_')
            match_string = '{}{}{}'.format(part_array[0],
                                           part_array[2],
                                           part_array[3])

            match_string += r'\d{2}T\d-SC\d{14}\.tar\.gz'

            for file_name in os.listdir(os.path.join('.', directory)):
                # Use a regex to match the converted product name
                # If it is found, remove it from the list (if it isn't already)
                if re.match(match_string, file_name):
                    print('This product already exists in the download folder.')
                    if product_name in not_exist_set:
                        print('Removing {} from products to order.'.format(product_name))
                        not_exist_set.remove(product_name)

        return not_exist_set
