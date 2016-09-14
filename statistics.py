import itertools
from decimal import Decimal

from helpers import timeit

from constants import SCALR_META
from constants import COST
from constants import OBJECT_TYPES


def is_valid_for_calculations_csv_row(row):
    return row.get(SCALR_META) and Decimal(row.get(COST, '0'))


def parse_object_ids_from_scalr_meta(string):
    objects_id = string.split(':')[1:]  # assume that every meta data string is valid
    if all(objects_id):
        return objects_id
    return []


@timeit
def calculate_statistics(csv_reader):
    statistics = {}
    # skip headers row
    csv_reader.next()
    for row in csv_reader:
        if not is_valid_for_calculations_csv_row(row):
            continue

        objects_id = parse_object_ids_from_scalr_meta(row[SCALR_META])
        if not objects_id:
            continue

        get = statistics.get
        for entity in itertools.izip(OBJECT_TYPES, objects_id):
            statistics[entity] = get(entity, 0.0) + float(row[COST])

#    for k in statistics:
#        statistics[k] = Decimal(statistics[k])
    return statistics
