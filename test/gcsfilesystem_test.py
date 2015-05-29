import unittest
from gcs import GcsFileSystem
from yaml import Loader
import yaml

import logging

class GcsFileSystemTest(unittest.TestCase):
    conf = yaml.load(file('./conf/gcs.yaml'), Loader=Loader)

    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')

    def test_connect(self):
        self.assertNotEqual(GcsFileSystem(conf=GcsFileSystemTest.conf), None)

    def test_it_should_split_bucket_from_key(self):
        bucket, key = GcsFileSystem._path_to_bucket_and_key("gcs://bucket/some/long/long/key")

        self.assertEqual(bucket, 'bucket')
        self.assertEqual(key, 'some/long/long/key')

    def test_it_should_put_file_as_a_single_chunk_and_then_remove_it(self):
        file_element = "gcs://{}/test/sample_upload".format(GcsFileSystemTest.conf['gcs']['bucket'])
        fs = GcsFileSystem(conf=GcsFileSystemTest.conf)

        self.assertFalse(fs.exists(file_element))

        self.assertEqual(fs.put('./data/sample.txt', file_element),
                         (GcsFileSystemTest.conf['gcs']['bucket'], 'test/sample_upload'))
        self.assertTrue(fs.exists(file_element))

        fs.remove(file_element)
        self.assertFalse(fs.exists(file_element))

    def test_it_should_put_file_as_a_multiply_chunks_and_then_remove_it(self):
        file_element = "gcs://{}/test/sample_upload_multipart".format(GcsFileSystemTest.conf['gcs']['bucket'])
        fs = GcsFileSystem(conf=GcsFileSystemTest.conf)

        self.assertFalse(fs.exists(file_element))

        self.assertEqual(fs.put_multipart('./data/picture.jpg', file_element, chunk_size=256*1024),
                         (GcsFileSystemTest.conf['gcs']['bucket'], 'test/sample_upload_multipart'))
        self.assertTrue(fs.exists(file_element))
        fs.remove(file_element)
        self.assertFalse(fs.exists(file_element))

    def test_it_should_find_files(self):
        # ensure you have /test/sample.txt file at your bucket
        not_found = "gcs://{}/some/hopefully/not/existing/key".format(GcsFileSystemTest.conf['gcs']['bucket'])
        file_element = "gcs://{}/test/sample".format(GcsFileSystemTest.conf['gcs']['bucket'])
        dir_element = "gcs://{}/test".format(GcsFileSystemTest.conf['gcs']['bucket'])
        root = "gcs://{}".format(GcsFileSystemTest.conf['gcs']['bucket'])

        fs = GcsFileSystem(conf=GcsFileSystemTest.conf)
        self.assertFalse(fs.exists(not_found))
        self.assertTrue(fs.exists(file_element))
        self.assertTrue(fs.exists(dir_element))
        self.assertTrue(fs.exists(root))
