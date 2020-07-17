import unittest
from log_processor import (
    load_file,
    get_unique_hostnames,
    get_largest_by_host_method,
    get_aggregate_res_size,
)


class TestLogProcessor(unittest.TestCase):
    def setUp(self):
        # self.processor = LogProcessor("log.txt")
        self.log = load_file("log.txt")
        self.unique_hostnames = {"hackernews.com", "google.com", "k8s.io"}
        self.top_two_largest = [
            ("google.com", "GET", 10000),
            ("hackernews.com", "GET", 3500),
        ]
        self.agg_res_size = {
            ("hackernews.com", "GET"): 3500,
            ("hackernews.com", "POST"): 20,
            ("google.com", "GET"): 10000,
            ("k8s.io", "GET"): 20,
        }

    def test_unique_hostnames(self):
        self.assertEqual(get_unique_hostnames(self.log), self.unique_hostnames)

    def test_largest_by_host_method(self):
        self.assertEqual(get_largest_by_host_method(self.log, 2), self.top_two_largest)

    def test_agg_res_size(self):
        self.assertEqual(get_aggregate_res_size(self.log), self.agg_res_size)


if __name__ == "__main__":
    unittest.main()
