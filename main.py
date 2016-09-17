import csv
import threading

import statistics
import utils
from classes import database
from classes.thread_pool import ThreadPool
from classes.url_zip_file_downloader import URLZipFileDownloader

MAX_THREADS = 5

FILES = [
    "https://www.dropbox.com/s/r0ctya6qn3xzr8j/d.csv.zip?dl=1",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-05.csv.zip",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-06.csv.zip",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-07.csv.zip",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-08.csv.zip",
]

db_lock = threading.Lock()


@utils.timeit
def process_zip_file_by_url(zip_file_url):
    downloader = URLZipFileDownloader(zip_file_url)
    archived_file_object = utils.get_archived_file_object_from_zip_archive(downloader.get_zip_archive())
    csv_reader = csv.reader(archived_file_object)
    stats = statistics.calculate_statistics(csv_reader)
    utils.print_obj_size(stats, message='Thread statistics dict size: ')
    return stats


def main():
    pool = ThreadPool(MAX_THREADS)

    for url in FILES[:]:
        pool.add_task(process_zip_file_by_url, url)

    pool.wait_completion()
    stats_list = pool.results

    stats = utils.merge_threads_statistics(stats_list)

    db = database.Database()
    db.save_statistics(stats)
    del db

if __name__ == '__main__':
    main()
