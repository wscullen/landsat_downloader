import unittest
from pathlib import Path
from landsat_downloader import l8_downloader_v2
import datetime
import os
import json
from landsat_downloader.test.timeit_dec import timeit
from landsat_downloader.utils import ConfigValueMissing, ConfigFileProblem

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




if __name__ == "__main__":
    unittest.main()
