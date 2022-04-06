import unittest
from pathlib import Path
from landsat_downloader import l8_downloader_v2
import datetime
import os
import json
from landsat_downloader.test.timeit_dec import timeit
from landsat_downloader.utils import ConfigValueMissing, ConfigFileProblem
import time

TEST_DIR = Path(__file__).absolute().parent


class TestL8Downloader(unittest.TestCase):
    def setUp(self):
        self.config_path = Path(TEST_DIR.parent.parent, "config.yaml")

        self.test_footprint_1 = "POLYGON((-110.05657077596709 49.01153105441047,-109.99065280721709 53.26747451320431,-110.25432468221709 54.1903679577336,-111.08928561971709 54.34435309167866,-113.59416843221709 54.061607249134155,-114.75871921346709 54.65060027562556,-115.85735202596709 55.16847874827975,-117.28557468221709 55.66732588602491,-119.35100436971709 55.543204417173975,-119.98821140096709 54.59971836352182,-118.31828952596709 53.867715218704966,-116.82414890096709 53.228032103413625,-115.96721530721709 52.23011549549104,-114.78069186971709 51.27802785817801,-114.27063874778048 49.871533466398866,-114.16077546653048 49.27308564086286,-113.63343171653048 48.97110715942032,-110.05657077596709 49.01153105441047))"
        self.geojson_string = '{ "type": "Polygon", "coordinates": [ [ [ -112.7795, 49.9322 ], [ -112.4311, 49.9310 ], [ -112.4293, 49.6083 ], [ -112.7749, 49.6119 ], [ -112.7768, 49.6843 ], [ -112.7795, 49.9322 ] ] ] }'
        self.usgs_format_geojson_string = '{"type": "Polygon", "coordinates": [{"longitude": -112.7795, "latitude": 49.9322}, {"longitude": -112.4311, "latitude": 49.931}, {"longitude": -112.4293, "latitude": 49.6083}, {"longitude": -112.7749, "latitude": 49.6119}, {"longitude": -112.7768, "latitude": 49.6843}, {"longitude": -112.7795, "latitude": 49.9322}]}'

    @timeit
    def test_geojson_to_usgs_format(self):

        downloader_obj = l8_downloader_v2.L8Downloader(self.config_path)

        results = downloader_obj.geojson_to_usgs_format(self.geojson_string)

        print(results)
        usgs_format_json_string = json.dumps(results)
        self.assertEquals(usgs_format_json_string, self.usgs_format_geojson_string)

    def test_login(self):
        downloader = l8_downloader_v2.L8Downloader(self.config_path)

        downloader.login()

    def test_dataset_search(self):
        # {'date_start': datetime.datetime(2020, 6, 1, 0, 0), 'date_end': datetime.datetime(2020, 6, 30, 23, 59, 59, 999999), 'cloud_percent': 100, 'collection_category': ['T1', 'T2']}

        downloader = l8_downloader_v2.L8Downloader(self.config_path)

        dataset_results = downloader.dataset_search("LANDSAT")

        print(dataset_results)

    def test_scene_search(self):
        downloader = l8_downloader_v2.L8Downloader(self.config_path)

        scene_results = downloader.scene_search(
            "landsat_8_c1",
            start_date="2021-07-01",
            end_date="2021-07-15",
            geojson=self.geojson_string,
            metadata_type="full",
        )

        print(scene_results)

    def test_download_options(self):
        downloader = l8_downloader_v2.L8Downloader(self.config_path)

        download_options = downloader.download_options(
            "LC90380262022081LGN00", "landsat_ot_c2_l2"
        )

        print(download_options)

    def test_download_request(self):
        downloader = l8_downloader_v2.L8Downloader(self.config_path)
        download_options = downloader.download_options(
            "LC90380262022081LGN00", "landsat_ot_c2_l2"
        )

        print(download_options)

        download_requests = []
        for option in download_options[0]["secondaryDownloads"]:
            if option["available"]:
                download_requests.append(
                    {
                        "entityId": option["entityId"],
                        "productId": option["id"],
                    }
                )

        download_results = downloader.download_request(
            download_requests,
            "test_download_request",
        )

        print(download_results)
        if len(download_results["availableDownloads"]) > 0:
            # download_results = downloader.download_request(
            #     download_results["availableDownloads"],
            #     "test_download_request",
            # )

            # print(download_results)
            for download in download_results["availableDownloads"]:
                downloader.download_file(download["url"])

    def test_download_retrieve(self):
        downloader = l8_downloader_v2.L8Downloader(self.config_path)
        download_results = downloader.download_retrieve(
            "LC08_L2SP_041025_20220319_20220329_02_T1"
        )

        while len(download_results["available"]) != 22:
            download_results = downloader.download_retrieve(
                "LC08_L2SP_041025_20220319_20220329_02_T1"
            )

            print(download_results)
            time.sleep(60)

        for download in download_results["available"]:
            downloader.download_file(download["displayId"], download["url"])

    def test_download_full_product(self):
        downloader = l8_downloader_v2.L8Downloader(self.config_path)

        product_name = "LC08_L2SP_041025_20220319_20220329_02_T1"
        entity_id = "LC80410252022078LGN00"
        dataset_name = "landsat_ot_c2_l2"
        download_path = Path.cwd()
        download_results = downloader.download_full_product(
            product_name, entity_id, dataset_name, download_path
        )

        print(download_results)

    def test_download_search(self):
        downloader = l8_downloader_v2.L8Downloader(self.config_path)

        product_name = "LC08_L2SP_039026_20220321_20220329_02_T1"
        download_results = downloader.download_search(product_name)

        print(download_results)


if __name__ == "__main__":
    unittest.main()
