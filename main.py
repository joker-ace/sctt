import csv
import threading

import statistics
import database
import zip_file_helpers

import helpers

FILES = [
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-05.csv.zip",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-06.csv.zip",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-07.csv.zip",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-08.csv.zip",
]

db_lock = threading.Lock()


@helpers.timeit
def process_files(files):
    #threads = []
    for file_name in files:
        thread = threading.Thread(target=process_zip_file_by_url, args=(file_name,), name=file_name[-16:])
        #threads.append(thread)
        thread.start()

    # completed_threads = []
    # while True:
    #     for i in threads:
    #         i.join(0.5)
    #         if not i.isAlive():
    #             completed_threads.append(i)
    #
    #     if len(threads) == len(completed_threads):
    #         break
    # del threads
    # del completed_threads

@helpers.timeit
def process_zip_file_by_url(zip_file_url):
    zip_archive = zip_file_helpers.download_zip_archive_to_memory(zip_file_url)
    archived_file_object = zip_file_helpers.get_archived_file_object_from_zip_archive(zip_archive)
    csv_reader = csv.DictReader(archived_file_object)
    stats = statistics.calculate_statistics(csv_reader)

    db_lock.acquire()
    db = database.Database()
    db.save_statistics(stats)
    db_lock.release()


def main():
    process_files(FILES)


if __name__ == '__main__':
    main()
