#!/usr/local/bin/python3
"""
A CLI utility for collecting quick statistics on a log file.
The log lines must be in the following format:
TS<int>:METHOD:https://example.com/some/path:<response size>
Example: TS1:GET:https://bitmex.com/volumes:1000
This program does not check the validity of log lines
For usage run with --help
"""

from typing import List, Tuple, Set, Dict
import heapq
import sys
import argparse


def load_file(file_name: str) -> List:
    """ 
    Takes a file_path and returns a List, discarding '\n'
    """
    try:
        with open(file_name, "r") as f:
            log_lines = f.read().splitlines()
            return log_lines
    except IOError:
        print(f"Could not open or read file: {file_name}")
        sys.exit()


def get_unique_hostnames(log: List) -> Set:
    """ 
    Takes a List of log lines, and returns a Set of unique hostnames
    """
    hostnames = []
    for line in log:
        host = line.split(":")[3].split("/")[2]
        hostnames.append(host)
    unique_hostnames = set(hostnames)
    return unique_hostnames


def get_aggregate_res_size(log: List) -> Dict[Tuple[str, str], int]:
    """
    Takes a List of log lines, and returns a Dict with keys of (host, method)
    and values of the aggregate response size of all requests in the List
    for that host & method pair
    """
    # For each entry in the log, parse the host, method and res_size
    # then create a a dictionary where the key is a tuple of (host, method)
    # and the value is the running sum of response_sizes for that host & method pair
    req_by_host_method = {}
    for line in log:
        host = line.split(":")[3].split("/")[2]
        method = line.split(":")[1]
        res_size = int(line.split(":")[4])
        if not (host, method) in req_by_host_method:
            req_by_host_method[(host, method)] = res_size
        else:
            current_res_size = req_by_host_method[(host, method)]
            req_by_host_method[(host, method)] = current_res_size + res_size
    return req_by_host_method


def get_largest_by_host_method(log: List, count: int) -> List[Tuple[str, str, int]]:
    """
    Takes a List of log lines and a count of the top n host and method pairs to return, 
    sorted by aggregate request size, and returns a List of Tuples (host, method, res_size)
    """
    res_sizes = get_aggregate_res_size(log)
    # nlargest returns a list of tuples [(host, method)]
    largest = heapq.nlargest(count, res_sizes, key=res_sizes.get)
    # for each item in the largest list, add the host, method and
    # aggregate response size to a list of tuples
    results = []
    for k in largest:
        results.append((k[0], k[1], res_sizes[(k[0], k[1])]))
    return results


if __name__ == "__main__":
    # Setup the CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("log_file", help="path to a log file in the required format")
    parser.add_argument(
        "mode", help="available modes: unique_hosts, agg_res_size, largest_res_by_host",
    )
    parser.add_argument(
        "-C",
        "--count",
        type=int,
        default=2,
        help="specify count when using largest_res_by_host mode",
    )
    args = parser.parse_args()

    # Load in the Log file
    log = load_file(args.log_file)

    # Execute the statistics based on the selected mode
    if args.mode == "unique_hosts":
        unique_hosts = get_unique_hostnames(log)
        for host in unique_hosts:
            print(host)

    elif args.mode == "agg_res_size":
        aggregate_res_size = get_aggregate_res_size(log)
        for k, v in aggregate_res_size.items():
            print(f"{k[0]} {k[1]} {v}")

    elif args.mode == "largest_res_by_host":
        largest_hosts = get_largest_by_host_method(log, args.count)
        for item in largest_hosts:
            print(f"{item[0]} {item[1]} {item[2]}")
    else:
        print(f"Invalid mode {args.mode} specified. See --help for valid modes")
        sys.exit()
