import itertools

from utils import timeit
from constants import SCALR_META
from constants import COST
from constants import OBJECT_TYPES


@timeit
def calculate_statistics(csv_reader):
    statistics = {}
    # skip headers row
    csv_reader.next()
    get = statistics.get
    for row in csv_reader:
        meta, cost = row[SCALR_META], float(row[COST])
        if not (meta and cost > 0.000000001):
            continue

        objects_id = meta.split(':')[1:]

        for entity in itertools.izip(OBJECT_TYPES, objects_id):
            if entity[1]:
                statistics[entity] = get(entity, 0.0) + cost

    return statistics
