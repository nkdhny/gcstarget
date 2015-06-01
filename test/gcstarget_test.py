import unittest
import logging

from yaml import Loader
import yaml

from luigicontrib.gcs import GcsFileSystem, GcsTarget


class GcsTargetTest(unittest.TestCase):
    conf = yaml.load(file('./conf/luigicontrib.yaml'), Loader=Loader)

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M')

    def test_it_should_perform_read_write_operations(self):
        fs = GcsFileSystem(conf=GcsTargetTest.conf)
        file_element = "luigicontrib://{}/test/target.txt".format(GcsTargetTest.conf['luigicontrib']['bucket'])

        self.assertFalse(fs.exists(file_element))

        target = GcsTarget(file_element, gcs_filesystem=fs)
        sample_data = open('./data/sample.txt', 'r').readlines()

        with target.open('w') as writable:
            for line in sample_data:
                print >> writable, line

        self.assertTrue(fs.exists(file_element))

        with target.open('r') as readable:
            idx = 0

            for line in readable:
                self.assertEqual(line, sample_data[idx])
                idx += 1

        fs.remove(file_element)
        self.assertFalse(fs.exists(file_element))
