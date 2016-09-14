import urllib2
import zipfile
import cStringIO

import helpers

@helpers.timeit
def download_zip_archive_to_memory(zip_file_url):
    zip_file_bytes = urllib2.urlopen(zip_file_url).read()
    buffer = cStringIO.StringIO(zip_file_bytes)
    return zipfile.ZipFile(buffer)

def get_archived_file_object_from_zip_archive(zip_archive):
    zipped_files = zip_archive.namelist()
    if zipped_files:
        csv_file_name = zipped_files[0]
        return zip_archive.open(csv_file_name)
    raise Exception('Exception: zip archive contain no files!')