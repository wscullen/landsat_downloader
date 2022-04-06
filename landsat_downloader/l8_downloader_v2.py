# -*- coding: utf-8 -*-
"""usgs_ee_downloader::l8_downloader.py

This module utilizes the USGS JSON API to query and download Landsat 8 and
Sentinel 2 data. More information can be found in the `USGS JSON API DOCS`_.

Example
-------
You create the L8Downloader class with the YAML config path as the only
argument to the constructor. The config file has ``USGS_USER`` and ``USGS_PASS``
params, which are used for authentication to the JSON api.

``L8Downloader`` is the class of the ``downloader``
module in the  ``landsatdownloader`` package::

    from landsatdownloader.downloader import L8Downloader

    dl_obj = L8Downloader('path/to/config.yaml')


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
from datetime import datetime, timedelta
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
import yaml
from pathlib import Path

from geomet import wkt

from . import utilities

from .transfer_monitor import TransferMonitor
from .utils import TaskStatus, ConfigFileProblem, ConfigValueMissing, AuthFailure


class L8Downloader:
    def __init__(
        self, path_to_config="config.yaml", username=None, password=None, verbose=False
    ):

        # create logger
        logger = logging.getLogger("L8Downloader")
        logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        self.logger = logger

        user_n = None
        pass_w = None

        if username and password:
            user_n = username
            pass_w = password
        else:
            # Load config from config.yaml
            try:
                with open(path_to_config, "r") as stream:
                    config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                self.logger.error("Problem loading config... exiting...")
                raise ConfigFileProblem
            except FileNotFoundError as e:
                self.logger.error(f"Missing config file with path {path_to_config}")
                raise e
            except BaseException as e:
                self.logger.error(
                    f"Unexpected problem occurred while loading config ({str(e)})"
                )

            required_config_keys = [
                "USGS_EE_USER",
                "USGS_EE_PASS",
            ]

            self.logger.debug(config.keys())

            try:
                config.keys()
            except AttributeError as e:
                raise ConfigFileProblem

            # Find the difference between sets
            # required_config_keys can be a sub set of config.keys()
            missing_keys = set(required_config_keys) - set(list(config.keys()))

            if len(list(missing_keys)) != 0:
                self.logger.error(
                    f"Config file loaded but missing critical vars, {missing_keys}"
                )
                raise ConfigValueMissing

            user_n = config["USGS_EE_USER"]
            pass_w = config["USGS_EE_PASS"]

        self.username = user_n
        self.password = pass_w

        if not (bool(self.username) and bool(self.password)):
            self.logger.error("Missing auth env vars, MISSING USERNAME OR PASSWORD")
            raise ConfigValueMissing

        self.url_template = "https://m2m.cr.usgs.gov/api/api/json/stable/{}"

        self.path_to_config = Path(path_to_config)

        self.auth_token = {"token": None, "last_active": None}

        self.max_attempts = 3
        self.initial_delay = 15
        self.api_timeout = 60 * 60

        self.verbose = verbose

    def login(self):
        """Check the json cache file for an active API key (obtained within the last 2 hours)

        if there is no valid API key, attempt to login and save the new API key

        Returns the API key (if login is successful)

        The API key is used by including the HTTP Header 'X-Auth-Token' in post requests.

        """
        authcache_path = "authcache.yaml"

        self.logger.info("Attempting login")

        if Path(authcache_path).exists():
            # the auth cache exists, check the time stamp
            # if the time delta between now and the timestamp is more than 2 hours, reauth
            try:
                with open(authcache_path, "r") as f:
                    authcache = yaml.safe_load(f)
            except yaml.YAMLError as exc:
                self.logger.error("Problem loading auth cache YAML")
                raise ConfigFileProblem
            except FileNotFoundError as e:
                self.logger.error(f"Missing auth cache file with path {authcache_path}")
                raise e
            except BaseException as e:
                self.logger.error("Unknown problem occurred while loading auth cache")

            auth_timestamp = authcache["timestamp"]
            auth_datetime = datetime.fromtimestamp(auth_timestamp)
            time_delta = datetime.now() - auth_datetime

            if time_delta > timedelta(hours=2):
                self.logger.info("API key has expired (older than 2 hours)")
                # the API key is more than 2 hours, attempt a login
                try:
                    auth_token = self.login_request()
                except AuthFailure as e:
                    self.logger.error(f"Authentication failed {str(e)}")
                else:
                    with open(authcache_path, "w") as f:
                        yaml.dump(auth_token, f, default_flow_style=False)

                    return auth_token["api_key"]
            else:
                return authcache["api_key"]
        else:
            # There is no auth cache file, attempt login and create
            try:
                auth_token = self.login_request()
            except AuthFailure as e:
                self.logger.error(f"Authentication failed {str(e)}")
            else:
                with open(authcache_path, "w") as f:
                    yaml.dump(auth_token, f, default_flow_style=False)

                    return auth_token["api_key"]

    def login_request(self):
        """Attempt a login to retrieve an API key using the username and password from the config"""
        data = {"username": self.username, "password": self.password}

        self.logger.debug(data)

        login_url = self.url_template.format("login")

        try:
            r = requests.post(login_url, json=data)
        except BaseException as e:
            self.logger.warning(
                f"There was a problem authenticating, connection to server failed. Exception: {str(e)}"
            )
            raise AuthFailure(str(e))
        else:
            result = r.json()

            self.logger.debug(result)

            if r.status_code == 200:
                if result["errorCode"]:
                    self.logger.warning(
                        f"Unable to authenticate, error: {result['error']}, errorInfo: {result['errorCode']}"
                    )
                    raise AuthFailure(str(e))
                else:
                    auth_token = {
                        "api_key": result["data"],
                        "timestamp": datetime.now().timestamp(),
                    }
                    return auth_token
            else:
                self.logger.warning(
                    f"There was a problem authenticating, status_code = {r.status_code}"
                )
                raise AuthFailure(r.status_code)

    def dataset_search(self, dataset_name_to_search):
        """
        {
            "datasetName": "Global",
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
            }
        }
        """
        api_key = self.login()

        if api_key:
            dataset_search_url = self.url_template.format("dataset-search")

            data = {
                "datasetName": dataset_name_to_search,
                # "catalog": "EE",
                # "includeMessages": False,
                # "publicOnly": True
            }

            headers = {"X-Auth-Token": api_key}

            try:
                r = requests.post(dataset_search_url, json=data, headers=headers)
            except BaseException as e:
                self.logger.error(
                    f"There was a problem with the request. Exception: {str(e)}"
                )
                raise e
            else:
                result = r.json()

                self.logger.debug(result)

                if r.status_code == 200:

                    if result["errorCode"]:
                        self.logger.warning(
                            f"There was a problem with the request, error: {result['errorCode']}, errorMessage: {result['errorMessage']}"
                        )
                    else:
                        return result["data"]

                else:
                    self.logger.warning(
                        f"There was a problem authenticating, status_code = {r.status_code}"
                    )
        else:
            self.logger.error("Unable to obtain API key for request.")

    def dataset_filters(self, dataset_name):
        """return the field attributes available for the specified dataset"""
        api_key = self.login()

        if api_key:
            dataset_search_url = self.url_template.format("dataset-filters")

            data = {"datasetName": dataset_name}

            headers = {"X-Auth-Token": api_key}

            try:
                r = requests.post(dataset_search_url, json=data, headers=headers)
            except BaseException as e:
                self.logger.error(
                    f"There was a problem with the request. Exception: {str(e)}"
                )
                raise e
            else:
                print(r.text)
                result = r.json()

                self.logger.debug(result)

                if r.status_code == 200:

                    if result["errorCode"]:
                        self.logger.warning(
                            f"There was a problem with the request, error: {result['errorCode']}, errorMessage: {result['errorMessage']}"
                        )
                    else:
                        return result["data"]

                else:
                    self.logger.warning(
                        f"There was a problem authenticating, status_code = {r.status_code}"
                    )
        else:
            self.logger.error("Unable to obtain API key for request.")

    def scene_search(
        self,
        dataset_name,
        scene_name_list=None,
        start_date=None,
        end_date=None,
        lower_left=None,
        upper_right=None,
        geojson=None,
        metadata_type="summary",
        max_cloud=100,
    ):
        """
        geojson must be dictionary of valid geojson structure,
        {
            "type": "polygon",
            "coordinates": coordinate[]
        }
        if geojson is specified, lower_left and upper_right are ignored.

        metadata_type can be summary or full
        """
        api_key = self.login()

        if api_key:
            dataset_search_url = self.url_template.format("scene-search")

            scene_filter = {
                "ingestFilter": None,
                "metadataFilter": None,
            }

            if start_date and end_date:
                scene_filter["acquisitionFilter"] = {
                    "start": start_date,
                    "end": end_date,
                }

            if lower_left and upper_right and not geojson:
                scene_filter["spatialFilter"] = {
                    "filterType": "mbr",
                    "lowerLeft": {
                        "latitude": lower_left[1],
                        "longitude": lower_left[0],
                    },
                    "upperRight": {
                        "latitude": upper_right[1],
                        "longitude": upper_right[0],
                    },
                }

            elif geojson:
                usgs_format_geojson = self.geojson_to_usgs_format(geojson)
                print(usgs_format_geojson)
                scene_filter["spatialFilter"] = {
                    "filterType": "geojson",
                    "geoJson": json.loads(geojson),
                }

            if max_cloud:
                scene_filter["cloudCoverFilter"] = {
                    "min": 0,
                    "max": max_cloud,
                    "includeUnknown": True,
                }

            if scene_name_list:
                self.logger.info(
                    "Metadata filters are not operational at this time, scene_name_list ignored..."
                )
                metadata_filter = {"filterType": "or", "childFilters": None}
                child_filters = []
                for name in scene_name_list:
                    name_filter = {
                        "filterType": "value",
                        "filterId": 20520,
                        "value": name,
                    }

                    child_filters.append(name_filter)

                metadata_filter["childFilters"] = json.dumps(child_filters)

                #     metadata_filter["childFilters"].append(name_filter)

                # scene_filter['metadataFilter'] = metadata_filter

            if not scene_name_list and not (upper_right and lower_left or geojson):
                self.logger.info(
                    "Missing essential search parameters, aborting search."
                )
                return None

            data = {
                "bulkListName": None,
                "metadataType": metadata_type,
                "orderListName": None,
                "startingNumber": 1,
                "compareListName": None,
                "excludeListName": None,
                "datasetName": dataset_name,
                "sceneFilter": scene_filter,
                "maxResults": 1000,
            }
            self.logger.debug(data)
            headers = {"X-Auth-Token": api_key}

            try:
                r = requests.post(dataset_search_url, json=data, headers=headers)
            except BaseException as e:
                self.logger.error(
                    f"There was a problem with the request. Exception: {str(e)}"
                )
                raise e
            else:
                if r.status_code == 200:
                    # print(r.text)
                    result = r.json()

                    if result["errorCode"]:
                        self.logger.warning(
                            f"There was a problem with the request, error: {result['errorCode']}, errorMessage: {result['errorMessage']}"
                        )
                    else:
                        return result["data"]["results"]

                else:
                    self.logger.warning(
                        f"There was a problem: status_code = {r.status_code}"
                    )

                    result = r.json()
                    if result["errorCode"]:
                        self.logger.warning(
                            f"There was a problem with the request, error: {result['errorCode']}, errorMessage: {result['errorMessage']}"
                        )
        else:
            self.logger.error("Unable to obtain API key for request.")

    def format_results(self, raw_results, platform_name):

        output_result_list = []

        if platform_name == "Landsat-8":

            for result in raw_results:
                product_dict = {}
                product_dict["entity_id"] = result["entityId"]

                product_dict["api_source"] = "usgs_ee"
                product_dict["footprint"] = result["spatialCoverage"]

                geom = ogr.CreateGeometryFromJson(json.dumps(product_dict["footprint"]))
                # Get Envelope returns a tuple (minX, maxX, minY, maxY)
                env = geom.GetEnvelope()

                def envelope_to_wkt(env_tuple):
                    coord1 = str(env_tuple[0]) + " " + str(env_tuple[3])
                    coord2 = str(env_tuple[1]) + " " + str(env_tuple[3])
                    coord3 = str(env_tuple[1]) + " " + str(env_tuple[2])
                    coord4 = str(env_tuple[0]) + " " + str(env_tuple[2])

                    wkt_string = "POLYGON(({}, {}, {}, {}, {}))".format(
                        coord1, coord2, coord3, coord4, coord1
                    )
                    return wkt_string

                product_dict["mbr"] = wkt.loads(envelope_to_wkt(env))

                product_dict["dataset_name"] = "landsat_8_c1"
                product_dict["name"] = result["displayId"]
                product_dict["uuid"] = result["entityId"]

                thumbnail_url = [
                    browse["thumbnailPath"]
                    for browse in result["browse"]
                    if browse["browseName"] == "LandsatLook Natural Color Preview Image"
                ][0]
                preview_url = [
                    browse["browsePath"]
                    for browse in result["browse"]
                    if browse["browseName"] == "LandsatLook Natural Color Preview Image"
                ][0]

                product_dict["thumbnail_url"] = thumbnail_url
                product_dict["preview_url"] = preview_url
                product_dict["options"] = [
                    key for key in result["options"].keys() if result["options"][key]
                ]
                product_dict["active"] = result["selected"]

                # 2017-05-25T15:17:11
                product_dict["published_date"] = result["publishDate"]

                product_dict["platform_name"] = platform_name

                path = [
                    metadata_entry["value"].strip()
                    for metadata_entry in result["metadata"]
                    if metadata_entry["fieldName"] == "WRS Path"
                ][0]
                row = [
                    metadata_entry["value"].strip()
                    for metadata_entry in result["metadata"]
                    if metadata_entry["fieldName"] == "WRS Row"
                ][0]

                # acquistion start, end
                start_time = [
                    metadata_entry["value"].strip()
                    for metadata_entry in result["metadata"]
                    if metadata_entry["fieldName"] == "Start Time"
                ][0]

                product_dict["acquisition_start"] = datetime.strptime(
                    start_time[:-1], "%Y:%j:%H:%M:%S.%f"
                )

                stop_time = [
                    metadata_entry["value"].strip()
                    for metadata_entry in result["metadata"]
                    if metadata_entry["fieldName"] == "Stop Time"
                ][0]

                # 2021:180:18:17:24.1376100
                product_dict["acquisition_end"] = datetime.strptime(
                    stop_time[:-1], "%Y:%j:%H:%M:%S.%f"
                )

                # TODO: Create a converter that converts PATH/ROW to MGRS and vice Versa  "fieldName":"WRS Path",
                product_dict["path"] = path
                product_dict["row"] = row
                product_dict["pathrow"] = path + row

                product_dict["mgrs"] = "TO DO"  # TODO: fix later

                # "fieldName":"Land Cloud Cover",
                land_cloud = [
                    metadata_entry["value"].strip()
                    for metadata_entry in result["metadata"]
                    if metadata_entry["fieldName"] == "Land Cloud Cover"
                ][0]
                scene_cloud = [
                    metadata_entry["value"].strip()
                    for metadata_entry in result["metadata"]
                    if metadata_entry["fieldName"] == "Scene Cloud Cover"
                ][0]

                product_dict["land_cloud_percent"] = land_cloud
                product_dict["scene_cloud_percent"] = scene_cloud
                product_dict["cloud_percent"] = scene_cloud
                utm_zone = [
                    metadata_entry["value"]
                    for metadata_entry in result["metadata"]
                    if metadata_entry["fieldName"] == "UTM Zone"
                ][0]

                product_dict["utm_zone"] = utm_zone

                product_dict["api_source"] = "usgs_ee_m2m"

                product_dict["vendor_name"] = "usgs_ee"

                product_dict["sat_name"] = "Landsat8"

                product_dict["summary"] = utm_zone = [
                    metadata_entry["value"]
                    for metadata_entry in result["metadata"]
                    if metadata_entry["fieldName"] == "Landsat Product Identifier"
                ][0]

                output_result_list.append(product_dict)

            return output_result_list

    def geojson_to_usgs_format(self, geojson_string):
        """Input is a valid geojson string, output is the USGS valid version for EE API

        the geojson string should be converted to a dict with the following structure:
        {
            type: string,
            coordinates: [
                {latitude: float,
                longitude: float},
                etc
            ]
        }
        """
        geojson_dict = json.loads(geojson_string)
        coordinates_list = geojson_dict["coordinates"][0]

        usgs_format_dict = {"type": geojson_dict["type"], "coordinates": []}

        for coord in coordinates_list:
            usgs_coord = {"longitude": coord[0], "latitude": coord[1]}
            usgs_format_dict["coordinates"].append(usgs_coord)

        return usgs_format_dict

    def download_options(self, entity_id: str, dataset_name: str):
        # Find the download options for these scenes
        # NOTE :: Remember the scene list cannot exceed 50,000 items!
        data = {"datasetName": dataset_name, "entityIds": entity_id}
        api_key = self.login()

        if api_key:
            url = self.url_template.format("download-options")

            self.logger.info(data)
            headers = {"X-Auth-Token": api_key}

            try:
                r = requests.post(url, json=data, headers=headers)
            except BaseException as e:
                self.logger.error(
                    f"There was a problem with the request. Exception: {str(e)}"
                )
                raise e
            else:
                if r.status_code == 200:
                    result = r.json()
                    if result["errorCode"]:
                        self.logger.warning(
                            f"There was a problem with the request, error: {result['errorCode']}, errorMessage: {result['errorMessage']}"
                        )
                    else:
                        return result["data"]

                else:
                    self.logger.warning(
                        f"There was a problem: status_code = {r.status_code}"
                    )
                    result = r.json()
                    if result["errorCode"]:
                        self.logger.warning(
                            f"There was a problem with the request, error: {result['errorCode']}, errorMessage: {result['errorMessage']}"
                        )
        else:
            self.logger.error("Unable to obtain API key for request.")

    def download_request(self, download_requests, label):
        payload = {
            "downloads": download_requests,
            "label": label,
        }
        api_key = self.login()

        if api_key:
            url = self.url_template.format("download-request")

            self.logger.info(payload)
            headers = {"X-Auth-Token": api_key}

            try:
                r = requests.post(url, json=payload, headers=headers)
            except BaseException as e:
                self.logger.error(
                    f"There was a problem with the request. Exception: {str(e)}"
                )
                raise e
            else:
                if r.status_code == 200:
                    result = r.json()
                    if result["errorCode"]:
                        self.logger.warning(
                            f"There was a problem with the request, error: {result['errorCode']}, errorMessage: {result['errorMessage']}"
                        )
                    else:
                        return result["data"]

                else:
                    self.logger.warning(
                        f"There was a problem: status_code = {r.status_code}"
                    )
                    result = r.json()
                    if result["errorCode"]:
                        self.logger.warning(
                            f"There was a problem with the request, error: {result['errorCode']}, errorMessage: {result['errorMessage']}"
                        )
        else:
            self.logger.error("Unable to obtain API key for request.")

    def download_retrieve(self, label):
        # Find the download options for these scenes
        # NOTE :: Remember the scene list cannot exceed 50,000 items!
        api_key = self.login()

        if api_key:
            headers = {"X-Auth-Token": api_key}

            url = self.url_template.format("download-retrieve")
            payload = {
                "label": label,
            }
            self.logger.info(payload)

            request_results = requests.post(url, json=payload, headers=headers)
            result = request_results.json()["data"]
            self.logger.debug(result)

            return result
        else:
            self.logger.error("Unable to obtain API key for request.")

    def download_file(self, download_path, filename, url, callback=None):
        # NOTE the stream=True parameter

        # self.check_auth() Since auth is baked into the url passed back from get
        # download url, the auth check is unnecessary
        self.logger.info("Trying to download the file...")

        try:
            r = requests.get(url, stream=True, timeout=2 * 60)

        except BaseException as e:
            self.logger.warning(str(e))
            return TaskStatus(
                False, "An exception occured while trying to download.", e
            )
        else:

            self.logger.debug(f"Response status code: {r.status_code}")
            self.logger.debug(r.headers)
            full_file_path = Path(
                download_path,
                (
                    filename
                    if filename
                    else r.headers["Content-Disposition"]
                    .split("filename=")[1]
                    .strip('"')
                ),
            )

            self.logger.info(f"Url created: {url}")
            self.logger.info(f"Full file path: {full_file_path}")
            file_size = int(r.headers["Content-Length"])
            transfer_progress = 0
            chunk_size = 1024 * 1024

            previous_update = 0
            update_throttle_threshold = 1  # Update every percent change

            if not os.path.isfile(full_file_path):
                try:
                    with open(full_file_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            f.write(chunk)
                            transfer_progress += chunk_size
                            transfer_percent = round(
                                min(100, (transfer_progress / file_size) * 100), 2
                            )
                            self.logger.debug(
                                f"Progress: {transfer_progress},  {transfer_percent:.2f}%"
                            )

                            self.logger.debug(str(transfer_percent - previous_update))
                            if (
                                transfer_percent - previous_update
                            ) > update_throttle_threshold:
                                if callback:
                                    self.logger.debug(
                                        "Calling task update state callback"
                                    )
                                    callback(
                                        transfer_progress, file_size, transfer_percent
                                    )

                                previous_update = transfer_percent

                except BaseException as e:
                    self.logger.debug(str(e))
                    return TaskStatus(
                        False, "An exception occured while trying to download.", e
                    )
                else:
                    return TaskStatus(True, "Download successful", str(full_file_path))
            else:
                return TaskStatus(
                    True,
                    "Requested file to download already exists.",
                    str(full_file_path),
                )

    def download_search(self, product_name):

        api_key = self.login()

        if api_key:
            headers = {"X-Auth-Token": api_key}

            url = self.url_template.format("download-search")
            payload = {
                "label": product_name,
            }
            self.logger.info(payload)

            request_results = requests.post(url, json=payload, headers=headers)
            result = request_results.json()["data"]
            self.logger.debug(result)

            return result

        else:
            self.logger.error("Unable to obtain API key for request.")

    def download_full_product(self, product_name, entity_id, dataset_name, working_dir):
        download_path = Path(working_dir, product_name)
        download_options = self.download_options(entity_id, dataset_name)

        self.logger.info(download_options)

        download_requests = []
        for option in download_options[0]["secondaryDownloads"]:
            if option["available"]:
                download_requests.append(
                    {
                        "entityId": option["entityId"],
                        "productId": option["id"],
                    }
                )

        request_results = self.download_request(
            download_requests,
            product_name,
        )

        self.logger.debug(request_results)
        if len(request_results["availableDownloads"]) == len(download_requests):
            os.mkdir(download_path)
            for download in request_results["availableDownloads"]:
                self.download_file(download_path, None, download["url"])

            return download_path

        max_attempts = 5
        attempts = 0

        retrieve_results = self.download_retrieve(product_name)
        self.logger.debug(retrieve_results)
        while (
            len(retrieve_results["available"]) != len(download_requests)
            and attempts < max_attempts
        ):
            self.logger.info(
                f"retrieve results: {len(retrieve_results['available'])}, of {len(download_requests)}"
            )
            retrieve_results = self.download_retrieve(product_name)
            attempts += 1
            time.sleep(60)

        if len(retrieve_results["available"]) == len(download_requests):
            os.mkdir(download_path)
            for download in retrieve_results["available"]:
                self.download_file(
                    download_path, download["displayId"], download["url"]
                )
            return download_path

        else:
            self.logger.error(
                "Unable to download all files. Attempts: {}".format(attempts)
            )
            return False


def authenticate(self):
    """Read the .json config file to get the user name and password"""

    # self.logger.debug('Attempting a direct login')
    # print(f'config path: {self.path_to_config}')
    auth_file = os.path.join(self.path_to_config, "authcache.json")
    now = time.time()

    if os.path.isfile(auth_file):
        # self.logger.debug('an authcache file is present')
        # print('found a auth cache file')
        with open(auth_file, "r") as infile:
            auth_token = json.load(infile)
            time_diff = now - auth_token["last_active"]

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
            "catalogId": "EE",
        }

        login_url = self.url_post_string.format("login")

        payload = {"jsonRequest": json.dumps(data)}

        try:
            r = requests.post(login_url, payload)
        except BaseException as e:
            self.logger.warning(
                f"There was a problem authenticating, connection to server failed. Exception: {str(e)}"
            )
            return None
        else:

            if r.status_code == 200:
                result = r.json()

                if result["error"] != "":
                    self.logger.warning(
                        f"Unable to authenticate, error: {result['error']}, errorInfo: {result['errorCode']}"
                    )
                    return None
                else:

                    self.auth_token["token"] = result["data"]
                    self.auth_token["last_active"] = time.time()

                    with open(auth_file, "w") as outfile:
                        json.dump(self.auth_token, outfile)

                    return self.auth_token

            else:
                self.logger.warning(
                    f"There was a problem authenticating, status_code = {r.status_code}"
                )
                return None

    def json_request(self):
        pass

    def auth_attempt(self):
        """Try to login, with exponential backup retry scheme"""

        # self.logger.debug('Attempting to login...')

        attempts = 0
        delay = self.initial_delay

        self.logger.debug("Trying to auth...")
        result = self.authenticate()
        self.logger.debug(f"Suth result {result}")

        while attempts < self.max_attempts:
            result = self.authenticate()
            if result:
                return result

            self.logger.warning("Problems authenticating, trying again after delay...")
            time.sleep(delay)
            delay *= 2
            attempts += 1

        # self.logger.debug('Auth not successful, giving up and exiting...')
        return None

    def check_auth(self):
        self.logger.info("Checking auth status...")
        now = time.time()

        if self.auth_token["token"]:
            time_diff = now - self.auth_token["last_active"]

            if time_diff > (self.api_timeout):
                self.logger.debug(
                    "Trying to authenticate again because auth token has timed out."
                )
                auth_result = self.auth_attempt()

                if auth_result:
                    return auth_result
                else:
                    raise AuthFailure("Cannot connect to auth api end point")

        else:
            self.logger.debug(
                "Trying to authenticate because there is no previous auth token."
            )
            auth_result = self.auth_attempt()

            if auth_result:
                return auth_result
            else:
                raise AuthFailure("Cannot connect to auth api end point")

    def update_auth_time(self):

        self.auth_token["last_active"] = time.time()

        auth_file = os.path.join(self.path_to_config, "authcache.json")

        with open(auth_file, "w") as outfile:
            json.dump(self.auth_token, outfile)

    def create_data_search_object_by_polygon(self, dataset_name, polygon, query_dict):
        self.check_auth()

        poly = ogr.CreateGeometryFromWkt(polygon)
        env = poly.GetEnvelope()

        # print "minX: %d, minY: %d, maxX: %d, maxY: %d" %(env[0],env[2],env[1],env[3])
        lowerleftX = env[0]
        lowerleftY = env[2]

        upperrightX = env[1]
        upperrightY = env[3]

        data = {
            "datasetName": dataset_name,
            "apiKey": self.auth_token["token"],
            "spatialFilter": {
                "filterType": "mbr",
                "lowerLeft": {"latitude": lowerleftY, "longitude": lowerleftX},
                "upperRight": {"latitude": upperrightY, "longitude": upperrightX},
            },
            "temporalFilter": {
                "startDate": query_dict["date_start"].strftime("%Y-%m-%d"),
                "endDate": query_dict["date_end"].strftime("%Y-%m-%d"),
            },
            "includeUnknownCloudCover": False,
        }

        if dataset_name == "LANDSAT_8_C1":

            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {
                        "filterType": "between",
                        "fieldId": 20522,
                        "firstValue": "0",
                        "secondValue": str(query_dict["cloud_percent"]),
                    },
                ],
            }
        elif dataset_name == "SENTINEL_2A":
            platform_name = "Sentinel-2"
            cloud_maximum_percent = query_dict["cloud_percent"]
            converted_cloud_max = math.floor(cloud_maximum_percent / 10) - 1
            print(converted_cloud_max)
            data["additionalCriteria"] = {
                "filterType": "and",
                "childFilters": [
                    {
                        "filterType": "between",
                        "fieldId": 18696,
                        "firstValue": "0",
                        "secondValue": str(converted_cloud_max),
                    },
                ],
            }

        return data
