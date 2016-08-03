'''
Streaming class for Gzip

Usage: 
To zip:
    gz_data = GzipStream().read(StringIO(data))

unzip:
    gz = gzip.GzipFile(fileobj=StringIO(gzip_output), mode='rb')
    gz.read()
'''

import gzip
from StringIO import StringIO

class GzipStream(StringIO):
    CHUNK_SIZE = 65536

    def __init__(self, name="data"):
        StringIO.__init__(self)

        self.source_eof = False
        self.gz_buffer = ""
        self.zipfile = gzip.GzipFile(name, 'wb', 9, self)

    def write(self, data):
        self.gz_buffer += data

    def read(self, source, size = -1):
        while ((len(self.gz_buffer) < size) or (size == -1)) and not self.source_eof:
            if source is None: 
                break
            chunk = source.read(GzipStream.CHUNK_SIZE)
            self.zipfile.write(chunk)
            if (len(chunk) < GzipStream.CHUNK_SIZE):
                self.source_eof = True
                self.zipfile.flush()
                self.zipfile.close()
                break

        if size == 0:
            result = ""
        if size >= 1:
            result = self.gz_buffer[0:size]
            self.gz_buffer = self.gz_buffer[size:]
        else:
            result = self.gz_buffer
            self.gz_buffer = ""

        return result
