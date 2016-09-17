import csv
import sys
from multiprocessing import Pool

import statistics
import utils
from classes import database
from classes.url_zip_file_downloader import URLZipFileDownloader

MAX_PARALLEL_FILES = 5

def process_zip_file_by_url(zip_file_url):
    downloader = URLZipFileDownloader(zip_file_url)
    archived_file_object = utils.get_archived_file_object_from_zip_archive(downloader.get_zip_archive())
    csv_reader = csv.reader(archived_file_object)
    stats = statistics.calculate_statistics(csv_reader)
    utils.print_obj_size(stats, message='Calculated statistics dict size: ')
    return stats


@utils.timeit
def main():
    pool = Pool(processes=MAX_PARALLEL_FILES)
    files = sys.stdin.read().split()
    processes_list = [pool.apply_async(process_zip_file_by_url, (f,)) for f in files]

    stats_list = [p.get() for p in processes_list]

    stats = utils.merge_statistics(stats_list)

    db = database.Database()
    db.save_statistics(stats)
    del db


if __name__ == '__main__':
    main()
