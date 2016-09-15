import tempfile
import shutil
import zipfile
from urllib2 import urlopen

import utils

class URLZipFileDownloader(object):
    def __init__(self, zip_file_url):
        self.zip_file_url = zip_file_url
        self.tmp_file_obj = None

    def __del__(self):
        del self.tmp_file_obj

    @utils.timeit
    def __download(self):
        response = urlopen(self.zip_file_url)
        self.tmp_file_obj = tempfile.NamedTemporaryFile()
        shutil.copyfileobj(response, self.tmp_file_obj)

    def get_zip_archive(self):
        if not self.tmp_file_obj:
            self.__download()
        return zipfile.ZipFile(self.tmp_file_obj)
