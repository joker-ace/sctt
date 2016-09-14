import sys
import itertools

from decimal import Decimal

from constants import MB
from constants import SCALR_META
from constants import COST
from constants import OBJECT_TYPES


def get_obj_size_in_mb(object):
    return sys.getsizeof(object) / MB


def print_obj_size(object):
    print object
    print '{0:.2f} MB\n'.format(get_obj_size_in_mb(object))


def get_archived_file_object_from_zip_archive(zip_archive):
    zipped_files = zip_archive.namelist()
    if zipped_files:
        csv_file_name = zipped_files[0]
        return zip_archive.open(csv_file_name)
    raise Exception('Exception: zip archive contain no files!')


def is_valid_for_calculations_csv_row(row):
    return row.get(SCALR_META) and Decimal(row.get(COST, '0'))

def parse_object_ids_from_scalr_meta(string):
    return string.split(':')[1:] # assume that every meta data string is valid

def calculate_statistics(csv_reader):
    statistics = {}
    csv_reader.next()
    for row in csv_reader:
        if not is_valid_for_calculations_csv_row(row):
            continue

        objects_id = parse_object_ids_from_scalr_meta(row[SCALR_META])
        for entity in itertools.izip(OBJECT_TYPES, objects_id):
            if entity not in statistics:
                statistics[entity] = Decimal('0')
            statistics[entity] += Decimal(row[COST])
    return statistics