import unittest
from pathlib import Path
from landsat_downloader import l8_downloader
import datetime
import os
import json
from landsat_downloader.test.timeit_dec import timeit

TEST_DIR = os.path.dirname(os.path.abspath(__file__))

class TestL8Downloader(unittest.TestCase):

    def setUp(self):
        self.test_footprint_1 = 'POLYGON((-110.05657077596709 49.01153105441047,-109.99065280721709 53.26747451320431,-110.25432468221709 54.1903679577336,-111.08928561971709 54.34435309167866,-113.59416843221709 54.061607249134155,-114.75871921346709 54.65060027562556,-115.85735202596709 55.16847874827975,-117.28557468221709 55.66732588602491,-119.35100436971709 55.543204417173975,-119.98821140096709 54.59971836352182,-118.31828952596709 53.867715218704966,-116.82414890096709 53.228032103413625,-115.96721530721709 52.23011549549104,-114.78069186971709 51.27802785817801,-114.27063874778048 49.871533466398866,-114.16077546653048 49.27308564086286,-113.63343171653048 48.97110715942032,-110.05657077596709 49.01153105441047))'
        self.test_footprint_2 = 'POLYGON((-117.219827586215 58.718199728247,-116.079310344835 58.8087169696263,-114.938793103456 58.6638893834194,-115.246551724145 58.1750962799711,-116.043103448283 57.7949238661779,-116.604310344835 57.4871652454883,-116.966379310353 56.99837214204,-116.71293103449 56.600096279971,-116.387068965525 56.6544066247986,-115.66293103449 56.3828549006606,-116.55 56.0750962799709,-115.409482758628 56.1294066247985,-115.029310344835 55.5863031765226,-114.268965517249 55.5681997282468,-113.834482758628 55.5138893834192,-113.418103448283 55.2423376592812,-113.182758620697 55.2423376592812,-112.838793103456 55.2785445558329,-112.494827586214 55.5681997282468,-112.168965517249 55.1880273144536,-111.915517241387 55.1699238661778,-111.553448275869 54.7716480041088,-111.028448275869 54.7897514523846,-110.304310344835 54.7897514523846,-109.869827586214 54.5725100730743,-109.88793103449 49.9380273144533,-109.942241379318 48.4897514523842,-112.150862068973 48.4535445558325,-113.707758620697 48.5621652454877,-114.196551724145 48.9785445558325,-114.685344827594 49.3949238661774,-114.92068965518 49.8113031765222,-114.631034482766 50.245785935143,-114.757758620697 50.770785935143,-115.355172413801 50.9699238661775,-115.101724137939 51.2414755903154,-115.22844827587 51.6578549006603,-115.318965517249 52.092337659281,-115.66293103449 52.0742342110051,-115.789655172421 52.5992342110052,-115.536206896559 52.8526824868673,-115.681034482766 53.3052686937638,-115.916379310352 53.3052686937638,-116.930172413801 53.3414755903156,-116.893965517249 53.812165245488,-116.169827586215 54.4095790385915,-114.812068965525 54.6992342110053,-114.323275862076 55.006992831695,-114.631034482766 54.9345790385915,-115.608620689663 55.0613031765226,-116.169827586215 55.006992831695,-116.676724137939 54.8078549006605,-117.002586206904 54.4095790385915,-117.76293103449 54.8078549006605,-118.414655172422 54.9888893834191,-119.175 54.7354411075571,-119.989655172422 54.8621652454881,-119.989655172422 56.6181997282468,-119.319827586215 56.6906135213503,-118.450862068973 56.726820417902,-117.889655172422 56.7630273144537,-117.925862068973 56.9259583489365,-117.744827586215 57.3061307627296,-117.962068965525 57.5776824868676,-118.052586206904 57.921648004109,-117.437068965525 58.04837214204,-117.292241379318 58.2475100730745,-117.219827586215 58.718199728247))'
        self.test_footprint_3_lethbridge = 'POLYGON((-113.09814998548927 50.04236546243994,-113.18878719252052 49.904583052743654,-113.15582820814552 49.78236964180449,-112.57080623548927 49.748662413160886,-112.44721004408302 49.84262935403644,-112.51312801283302 49.998247389145405,-112.73834773939552 50.07586810521376,-113.09814998548927 50.04236546243994))'
        self.test_footprint_2_result_list = ['12U', '11U', '11V']
        self.test_footprint_2_result_list2 = ['12UWV', '12UWU', '12UWF', '12UWE', '12UWD', '12UWC', '12UWB', '12UWA', '12UVV', '12UVU', '12UVG', '12UVF', '12UVE', '12UVD', '12UVC', '12UVB', '12UVA', '12UUV', '12UUU', '12UUG', '12UUF', '12UUE', '12UUD', '12UUC', '12UUB', '12UUA', '12UTV', '12UTU', '12UTE', '12UTD', '12UTC', '12UTB', '12UTA', '11UQV', '11UQU', '11UQT', '11UQS', '11UQR', '11UQQ', '11UPV', '11UPU', '11UPT', '11UPS', '11UPR', '11UPQ', '11VPF', '11VPE', '11UPC', '11VPC', '11UPB', '11UPA', '11UNV', '11UNU', '11UNT', '11VNF', '11VNE', '11VND', '11UNC', '11VNC', '11UNB', '11UNA', '11VMF', '11VME', '11VMD', '11UMC', '11VMC', '11UMB', '11UMA', '11ULC', '11VLC', '11ULB', '11ULA']
        self.test_footprint_2_result_list_single = ['12UWV', '12UWU', '12UWF', '12UWE', '12UWD', '12UWC', '12UWB', '12UWA', '12UVV', '12UVU', '12UVG', '12UVF', '12UVE', '12UVD', '12UVC', '12UVB', '12UVA', '12UUV', '12UUU', '12UUG', '12UUF', '12UUE', '12UUD', '12UUC', '12UUB', '12UUA', '12UTV', '12UTU', '12UTE', '12UTD', '12UTC', '12UTB', '12UTA']

        self.tile_list_small = ['11UQR']  # by lethbridge

        self.config_path = Path(TEST_DIR, 'test_data', 'l8downloader.config.json')
        print(self.config_path)
        self.downloader_obj = l8_downloader.L8Downloader(self.config_path)

        self.SENTINEL2_DATASET_NAME = 'SENTINEL_2A'
        self.SENTINEL2_PLATFORM_NAME = 'Sentinel-2'
        self.LANDSAT8_DATASET_NAME = 'LANDSAT_8_C1'
        self.LANDSAT8_PLATFORM_NAME = 'Landsat-8'

        self.date_start = datetime.date(2018, 8, 1)
        self.date_end = datetime.date(2018, 8, 31)

        self.path_to_intermediate_query_results_usgs_ee = Path(TEST_DIR,
                                                               'test_data',
                                                               'raw_results_query_by_tile_11UQR.json',)
        self.path_to_cleaned_query_results_usgs_ee = Path(TEST_DIR,
                                                          'test_data',
                                                          'clean_results_query_by_tile_11UQR.json',)

        self.max_cloud = 100

        self.QUERY_DICT_EXAMPLE = {
            'footprint': self.test_footprint_2,
            'date_start': self.date_start,
            'date_end': self.date_end,
            'cloud_percent': self.max_cloud,
        }

        self.query_dict_example_single_tile = {
            'footprint': self.test_footprint_3_lethbridge,
            'date_start':  datetime.date(2018, 8, 22),
            'date_end': datetime.date(2018, 8, 22),
            'cloud_percent': self.max_cloud,
        }


    def test_search_for_products_polygon_to_tiles_sentinel3(self):
        results = self.downloader_obj.search_for_products_polygon_to_tiles(self.SENTINEL2_DATASET_NAME,
                                                                self.test_footprint_1,
                                                                self.QUERY_DICT_EXAMPLE,
                                                                detailed=True)

        print(len(results))
        self.assertTrue(True)

    def test_search_for_products_polygon_to_tiles_abagextent(self):
        results = self.downloader_obj.search_for_products_polygon_to_tiles(self.SENTINEL2_DATASET_NAME,
                                                                self.test_footprint_1,
                                                                self.QUERY_DICT_EXAMPLE,
                                                                detailed=True)

        print(len(results))
        self.assertTrue(True)

    def test_search_for_products_polygon_to_tiles_sentinel2(self):
        results = self.downloader_obj.search_for_products(self.SENTINEL2_DATASET_NAME,
                                                          self.test_footprint_3_lethbridge,
                                                          self.QUERY_DICT_EXAMPLE)

        print(len(results))
        print(results[0])
        print(results)
        single_element_list = []
        single_element_list.append(results[0])
        print(single_element_list)
        # populated_result = self.downloader_obj.populate_result_list(single_element_list,
        #                                                             self.SENTINEL2_PLATFORM_NAME,
        #                                                             self.SENTINEL2_DATASET_NAME)
        # print(populated_result)
        self.assertTrue(True)

    @timeit
    def test_get_dataset_metadata_info_sentinel2(self):

        results = self.downloader_obj.get_dataset_field_ids(self.SENTINEL2_DATASET_NAME)

        # print(results)
        self.assertTrue(True)

    @timeit
    def test_populate_result_list(self):
        # Load previous results
        json_results = None
        with open(self.path_to_intermediate_query_results_usgs_ee, 'r') as json_file:
            json_results = json.load(json_file)

        print(json_results)

        cleaned_results = self.downloader_obj.populate_result_list(json_results, self.SENTINEL2_PLATFORM_NAME, self.SENTINEL2_DATASET_NAME, detailed=False)

        cleaned_results_compare = None
        with open(self.path_to_cleaned_query_results_usgs_ee, 'r') as outfile:
            clean_results_compare = json.load(outfile)

        self.assertEqual(clean_results_compare, cleaned_results)

    @timeit
    def test_search_scene_metadata(self):
        json_results = None
        with open(self.path_to_intermediate_query_results_usgs_ee, 'r') as json_file:
            json_results = json.load(json_file)

        data_results = json_results['data']['results']
        just_entity_ids = [d['entityId'] for d in data_results]

        print(just_entity_ids)

        detailed_metadata_results = self.downloader_obj.search_scene_metadata(self.SENTINEL2_DATASET_NAME,
                                                                              just_entity_ids)

        print(detailed_metadata_results)
        self.assertEqual(len(detailed_metadata_results), 12)

    @timeit
    def test_search_for_products_by_tile(self):
        results = self.downloader_obj.search_for_products_by_tile(self.SENTINEL2_DATASET_NAME,
                                                        self.tile_list_small,
                                                        self.QUERY_DICT_EXAMPLE,
                                                        detailed=True)

        print(results)
        self.assertEqual(len(results), 12)

    def test_alberta_ag_extent(self):

        results = self.downloader_obj.search_for_products_polygon_to_tiles(
                    self.SENTINEL2_DATASET_NAME,
                    self.test_footprint_2,
                    self.QUERY_DICT_EXAMPLE,
                    detailed=True)

        self.assertEqual(len(results), 988)

    @timeit
    def test_search_for_products_by_tile_detailed(self):
        results = self.downloader_obj.search_for_products_by_tile(self.SENTINEL2_DATASET_NAME,
                                                        self.tile_list_small,
                                                        self.query_dict_example_single_tile,
                                                        detailed=True)

        print(results)
        self.assertEqual(len(results), 1)

    @timeit
    def test_search_for_products_by_tile_not_detailed(self):
        results = self.downloader_obj.search_for_products_by_tile(self.SENTINEL2_DATASET_NAME,
                                                        self.tile_list_small,
                                                        self.query_dict_example_single_tile,
                                                        detailed=False)

        print(results)
        self.assertEqual(len(results), 1)

    # def test_find_mgrs_intersection_coarse(self):
    #     result_list = utilities.find_mgrs_intersection_large(self.test_footprint_2)

    #     self.assertEqual(set(result_list), set(self.test_footprint_2_result_list))

    # def test_find_mgrs_intersection_fine_single(self):

    #     single_gzd = '12U'

    #     fine_result_list_single = utilities.find_mgrs_intersection_100km_single(self.test_footprint_2,
    #                                                                             single_gzd)

    #     self.assertEqual(set(fine_result_list_single), set(self.test_footprint_2_result_list_single))

    # def test_find_mgrs_intersection_fine(self):

    #     gzd_initial_list = self.test_footprint_2_result_list

    #     fine_result_list = utilities.find_mgrs_intersection_100km(self.test_footprint_2,
    #                                                               gzd_initial_list)

    #     self.assertEqual(set(fine_result_list), set(self.test_footprint_2_result_list2))


if __name__ == '__main__':
    unittest.main()