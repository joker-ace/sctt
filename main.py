import csv

import helpers
from constants import CSV_DICT_READER_FIELDS

from classes.url_zip_file_reader import URLZipFileReader

FILES = [
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-05.csv.zip",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-06.csv.zip",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-07.csv.zip",
    "https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-08.csv.zip",
]


def main():
    file_uri = FILES[0]

    zip_archive = URLZipFileReader(file_uri).get_zip_file_object()
    file_object = helpers.get_archived_file_object_from_zip_archive(zip_archive)
    csv_reader = csv.DictReader(file_object, fieldnames=CSV_DICT_READER_FIELDS)
    result = helpers.calculate_statistics(csv_reader)

    print result


if __name__ == '__main__':
    main()
