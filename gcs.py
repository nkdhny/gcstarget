import os
from urlparse import urlsplit
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import luigi.target
from luigi.target import FileSystemException
from oauth2client.client import SignedJwtAssertionCredentials
import logging
from retrying import retry


class GcsFileSystem(luigi.target.FileSystem):
    logger = logging.getLogger('GcsFileSystem')

    MIN_CHUNK_SIZE = 256 * 1024

    def __init__(self, secret_key=None, email=None, private_key_password=None, conf=None):

        secret_key = secret_key or conf['auth']['root']['cert']
        email = email or conf['auth']['root']['email']
        private_key_password = private_key_password or conf['auth']['root']['password']

        with open(secret_key) as f:
            private_key = f.read()

        credentials = SignedJwtAssertionCredentials(email, private_key,
                                                    "https://www.googleapis.com/auth/devstorage.read_write",
                                                    private_key_password=private_key_password)

        self.gcs_service = discovery.build('storage', 'v1', credentials=credentials)

    def isdir(self, path):
        (bucket, key) = GcsFileSystem._path_to_bucket_and_key(path)

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
        (bucket, key) = GcsFileSystem._path_to_bucket_and_key(path)

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

        (bucket, key) = self._path_to_bucket_and_key(path)

        if self._is_root(key):
            raise FileSystemException("Can't remove root of the bucket {}".format(bucket))

        gcs_object = GcsFileSystem._get_object(self.gcs_service, bucket, key, GcsFileSystem.logger)

        if gcs_object:
            self.gcs_service.objects().delete(bucket=bucket, object=key).execute()
            return True

        elif recursive and self.isdir(key):
            gcs_objects = self.gcs_service.objects().list(bucket=bucket,
                                                          prefix=GcsFileSystem._add_path_delimiter(key)).execute()
            for gcs_object in gcs_objects['items']:
                self.gcs_service.objects().delete(bucket=bucket, object=gcs_object['name']).execute()

            return True

        else:
            raise FileSystemException("Can't remove dir {} without recursive flag".format(key))

    @staticmethod
    def _path_to_bucket_and_key(path):
        (scheme, netloc, path, query, fragment) = urlsplit(path)
        path_without_initial_slash = path[1:]
        return netloc, path_without_initial_slash

    def put(self, local_path, destination_gcs_path):
        """
        Put an object stored locally to an GCS path.
        """
        (bucket, key) = GcsFileSystem._path_to_bucket_and_key(destination_gcs_path)

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

        (bucket, key) = self._path_to_bucket_and_key(destination_gcs_path)

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
