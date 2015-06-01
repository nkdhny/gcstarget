import os
import logging
import random
import tempfile

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import luigi.target
from oauth2client.client import SignedJwtAssertionCredentials
from retrying import retry
import sys
import yaml
from yaml import Loader

try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit

from luigi.format import get_default_format, MixedUnicodeBytes
from luigi.target import FileSystemException, FileSystemTarget, AtomicLocalFile


class GcsFileSystem(luigi.target.FileSystem):
    logger = logging.getLogger('GcsFileSystem')

    MIN_CHUNK_SIZE = 256 * 1024

    def __init__(self, secret_key=None, email=None, private_key_password=None, conf=None):

        try:
            conf = conf or yaml.load(file('/etc/gaw/luigicontrib.yaml'), Loader=Loader)
        except:
            GcsFileSystem.logger.warn("No config provided")
        secret_key = secret_key or conf['auth']['root']['cert']
        email = email or conf['auth']['root']['email']
        private_key_password = (private_key_password or conf['auth']['root']['password']) or "notasecret"

        if not secret_key or not email:
            GcsFileSystem.logger.error(
                "GCS user email and secret key must be provided either in config or in parameters")
            raise Exception("GCS user email and secret key must be provided either in config or in parameters")

        with open(secret_key) as f:
            private_key = f.read()

        credentials = SignedJwtAssertionCredentials(email, private_key,
                                                    "https://www.googleapis.com/auth/devstorage.read_write",
                                                    private_key_password=private_key_password)

        self.gcs_service = discovery.build('storage', 'v1', credentials=credentials)

    def isdir(self, path):
        (bucket, key) = GcsFileSystem.path_to_bucket_and_key(path)

        if GcsFileSystem._is_root(key):
            return True

        objects_prefixed_by_key = self.gcs_service.objects().list(bucket=bucket,
                                                                  prefix=GcsFileSystem._add_path_delimiter(
                                                                      key)).execute()

        if 'items' in objects_prefixed_by_key and objects_prefixed_by_key['items']:
            return True

        return False

    @staticmethod
    def _is_root(key):
        return (len(key) == 0) or (key == '/')

    @staticmethod
    def _add_path_delimiter(key):
        return key if key[-1:] == '/' else key + '/'

    def mkdir(self, path, parents=True, raise_if_exists=False):
        pass

    def exists(self, path):
        (bucket, key) = GcsFileSystem.path_to_bucket_and_key(path)

        gcs_object = GcsFileSystem._get_object(self.gcs_service, bucket, key, GcsFileSystem.logger)

        if gcs_object:
            return True
        elif self.isdir(path):
            return True
        else:
            return False

    @staticmethod
    def _get_object(gcs_service, bucket, key, logger):
        gcs_object = None
        logger.debug("Looking for {}/{} in GCS".format(bucket, key))
        try:
            gcs_object = gcs_service.objects().get(bucket=bucket, object=key).execute()
        except HttpError as http:
            if http.resp.status != 404:
                logger.error(http)
                raise http
            else:
                logger.debug("Object {}/{} not found in GCS".format(bucket, key))

        logger.debug("Found {}/{} in GCS".format(bucket, key))
        return gcs_object

    def remove(self, path, recursive=True, skip_trash=True):

        if not self.exists(path):
            return False

        (bucket, key) = self.path_to_bucket_and_key(path)

        if self._is_root(key):
            raise FileSystemException("Can't remove root of the bucket {}".format(bucket))

        gcs_object = GcsFileSystem._get_object(self.gcs_service, bucket, key, GcsFileSystem.logger)

        if gcs_object:
            self.gcs_service.objects().delete(bucket=bucket, object=key).execute()
            return True

        elif recursive and self.isdir(path):
            gcs_objects = self.gcs_service.objects().list(bucket=bucket,
                                                          prefix=GcsFileSystem._add_path_delimiter(key)).execute()
            for gcs_object in gcs_objects['items']:
                self.gcs_service.objects().delete(bucket=bucket, object=gcs_object['name']).execute()

            return True

        else:
            raise FileSystemException("Can't remove dir {} without recursive flag".format(key))

    @staticmethod
    def path_to_bucket_and_key(path):
        (scheme, netloc, path, query, fragment) = urlsplit(path)
        path_without_initial_slash = path[1:]
        return netloc, path_without_initial_slash

    def put(self, local_path, destination_gcs_path):
        """
        Put an object stored locally to an GCS path.
        """
        (bucket, key) = GcsFileSystem.path_to_bucket_and_key(destination_gcs_path)

        self.gcs_service.objects().insert(media_body=local_path, name=key, bucket=bucket).execute()
        return bucket, key

    def put_multipart(self, local_path, destination_gcs_path, chunk_size=67108864):
        """
        Put an object stored locally to an GCS path
        using using MediaFileUpload chunks(for files > 5GB,
        see https://developers.google.com/api-client-library/python/guide/media_upload).

        :param local_path: Path to source local file
        :param destination_gcs_path: URL for target GCS location
        :param chunk_size: Chunk size in bytes. Default: 67108864 (64MB).
            Chunk size restriction: There are some chunk size restrictions based on the size of the file
            you are uploading. Files larger than 256 KB (256 * 1024 B) must have chunk sizes that are
            multiples of 256 KB. For files smaller than 256 KB, there are no restrictions. In either case,
            the final chunk has no limitations; you can simply transfer the remaining bytes.
        """

        source_size = os.stat(local_path).st_size

        if source_size <= chunk_size or source_size < GcsFileSystem.MIN_CHUNK_SIZE:
            GcsFileSystem.logger.debug("File too small will upload as a single chunk")
            return self.put(local_path, destination_gcs_path)

        chunk_size = GcsFileSystem.correct_chunk_size(chunk_size)

        (bucket, key) = self.path_to_bucket_and_key(destination_gcs_path)

        media = MediaFileUpload(local_path, chunksize=chunk_size, resumable=True)

        request = self.gcs_service.objects().insert(media_body=media, name=key, bucket=bucket)

        def should_retry(exception):
            return isinstance(exception, HttpError) and exception.resp.status in [500, 502, 503, 504]

        @retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, retry_on_exception=should_retry)
        def load_chunk(r):
            self.logger.debug("Uploading chunk to {}/{}".format(bucket, key))
            return r.next_chunk()

        response = None
        while response is None:
            upload_status, response = load_chunk(request)
            self.logger.debug("Chunk uploaded to {}/{}".format(bucket, key))
            if upload_status:
                self.logger.debug(
                    "Overall uploaded to {}/{}: {}%".format(bucket, key, int(upload_status.progress() * 100)))

        return bucket, key

    @staticmethod
    def correct_chunk_size(chunk_size):
        if chunk_size % GcsFileSystem.MIN_CHUNK_SIZE != 0:
            GcsFileSystem.logger.warn("Chunk size must be multiples of 256KB, will set to {}")
            return (chunk_size / GcsFileSystem.MIN_CHUNK_SIZE + 1) * GcsFileSystem.MIN_CHUNK_SIZE
        else:
            return chunk_size

    def read(self, name):
        bucket, key = self.path_to_bucket_and_key(name)
        return self.gcs_service.objects().get_media(bucket=bucket, object=key).execute()

    def open_read(self, name):
        return ReadableGcsFile(name, self)

    def open_write(self, name):
        return AtomicGcsFile(name)


class AtomicGcsFile(AtomicLocalFile):
    """
    An GCS file that writes to a temp file and put to S3 on close.
    """

    def __init__(self, path, fs):
        self.fs = fs
        super(AtomicGcsFile, self).__init__(path)

    def move_to_final_destination(self):
        self.fs.put_multipart(self.tmp_path, self.path)

    def generate_tmp_path(self, path):
        fileName, fileExtension = os.path.splitext(path)
        return os.path.join(tempfile.gettempdir(), 'luigi-luigicontrib-tmp-%09d.%s' % (random.randrange(0, 1e10), fileExtension))




class ReadableGcsFile:
    def __init__(self, name, fs):
        self.fs = fs
        self.name = name

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

    def __enter__(self):
        return self

    def __iter__(self):
        for line in self.fs.read(self.name).splitlines(True):
            yield line

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self


class GcsTarget(FileSystemTarget):
    fs = None

    def __init__(self, path, format=None, gcs_filesystem=None):
        super(GcsTarget, self).__init__(path)
        if format is None:
            format = get_default_format()

        # Allow to write unicode in file for retrocompatibility
        if sys.version_info[:2] <= (2, 6):
            format = format >> MixedUnicodeBytes

        self.format = format
        self.fs = gcs_filesystem or GcsFileSystem()

    def open(self, mode='r'):
        """
        """
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            if not self.fs.exists(self.path):
                raise FileSystemException("Could not find file at %s" % self.path)

            fileobj = self.fs.open_read(self.path)
            return self.format.pipe_reader(fileobj)
        else:
            return self.format.pipe_writer(AtomicGcsFile(self.path, self.fs))
