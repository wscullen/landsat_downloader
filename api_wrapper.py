from . import l8_downloader
import tqdm
import time
import logging

logger = logging.getLogger(__name__)


def query_by_polygon(dataset_name, wkt_polygon_list, arg_list, date_string, config_path=None):

    product_dict = {}
    result_dict = {}
    downloader = l8_downloader.L8Downloader(config_path, verbose=False)
    progress_iter = tqdm.tqdm(wkt_polygon_list)

    for index, fp in enumerate(progress_iter):
        try:
            print(f'Querying USGS_EE, looking for {dataset_name}')
            entity_list = downloader.search_for_products(
                dataset_name, wkt_polygon_list[index], arg_list)
            logger.debug(len(entity_list))
            for entity in entity_list:
                product_dict[entity['entity_id']] = entity

        except Exception as e:
            logger.debug(
                'Error occured while trying to query API: {}'.format(e))
            print(f'Sorry something went wrong while trying to query API. {e}')
            raise

        time.sleep(1)

    if product_dict:
        try:
            with_detailed_metadata = downloader.fill_detailed_metadata(
                list(product_dict.values()))
            for entity in with_detailed_metadata:
                result_dict[entity['entity_id']] = entity
        except Exception as e:
            logger.debug(
                'Error occured while trying to query API: {}'.format(e))
            print(f'Sorry something went wrong while trying to query API. {e}')
            raise
    else:
        result_dict = {}

    return result_dict


def query_by_name(dataset_name, name_list, arg_list, date_string, config_path=None):

    product_dict = {}
    result_dict = {}

    downloader = l8_downloader.L8Downloader(config_path, verbose=False)

    try:
        entity_list = downloader.search_for_products_by_name(
            dataset_name, name_list, arg_list)

        for entity in entity_list:
            product_dict[entity['entity_id']] = entity

    except Exception as e:
        logger.debug('Error occured while trying to query API: {}'.format(e))
        print('Sorry something went wrong while trying to query API')
        raise

    if product_dict:
        with_detailed_metadata = downloader.fill_detailed_metadata(
            list(product_dict.values()))
        for entity in with_detailed_metadata:
            result_dict[entity['entity_id']] = entity
    else:
        result_dict = {}

    return result_dict
