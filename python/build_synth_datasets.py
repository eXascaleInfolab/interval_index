import random
from numpy.random import normal, uniform
import numpy as np
import math
from heapq import heapify, heappush, heappop
import os

MIN = 0
MAX = 10000000
POINTS_COUNT = 1000000
QUERIES_COUNT = 200000


def create_path_silent(path):
    try:
        os.mkdir(path)
    except:
        pass   

#removes all previously built datasets in a directory
def clean_path(path):
    for file in os.listdir(path):
        if file.endswith(".txt"):
            os.remove(path + "/" + file)

def save_dataset(filename, intervals, queries):
    intervals_copy = [value for value in intervals]
    queries_copy = [value for value in queries]
    random.shuffle(intervals_copy)
    random.shuffle(queries_copy)
    out = open(filename, "w")
    out.write(str(len(intervals_copy)) + "\n")
    for index in xrange(len(intervals_copy)):
        start, length = intervals_copy[index]
        out.write(str(start) + "\t" + str(start + length) + "\t" + str(index + 1) + "\n")
    out.write(str(len(queries_copy)) + "\n")
    for start, length in queries_copy:
        out.write(str(start) + "\t" + str(start + length) + "\n")
    out.close()



create_path_silent("datasets")


if 1:
    # query_len
    print ".. datasets for query_time = f(query_len)"
    create_path_silent("datasets/query_len")
    clean_path("datasets/query_len")
    len_mean = 100
    len_stdev = 10
    query_len = 1
    intervals = []
    queries = []
    lengths = [length >=0 and length or 0.0 for length in normal(len_mean, len_stdev, POINTS_COUNT)]
    for point_index in xrange(POINTS_COUNT):
        start = random.random() * (MAX - MIN) + MIN
        length = lengths[point_index]
        intervals += [(start, length)]
    intervals.sort()
    overlappings = []
    started = []
    for start, length in intervals:
        while started:
            right_border = heappop(started)
            if right_border >= start:
                heappush(started, right_border)
                break
        overlappings += [len(started)]
        heappush(started, start + length)
    avg_overlapping = sum(overlappings) / float(len(overlappings))
    
    lengths = normal(100, 10, QUERIES_COUNT)
    DATASETS_COUNT = 30
    query_length = 10
    factor = math.exp(math.log(10000 / float(query_length) ) / (DATASETS_COUNT - 1))
    
    for length_factor in xrange(DATASETS_COUNT):
        queries = []
        for point_index in xrange(QUERIES_COUNT):
            start = random.random() * (MAX - MIN) + MIN
            queries += [(start, query_length)]
        save_dataset("datasets/query_len/dataset_query_len_%d.txt" % (query_length), intervals, queries)
        query_length =  math.ceil(query_length * factor)


if 0:
    print ".. datasets for query_time = f(checkpint_interval)"
    # chi_time_mem
    len_mean = 100
    len_stdev = 10
    intervals = []
    queries = []
    lengths = [length >=0 and length or 0.0 for length in normal(len_mean, len_stdev, POINTS_COUNT)]
    for point_index in xrange(POINTS_COUNT):
        start = random.random() * (MAX - MIN) + MIN
        length = lengths[point_index]
        intervals += [(start, length)]
    intervals.sort()
    overlappings = []
    started = []
    for start, length in intervals:
        while started:
            right_border = heappop(started)
            if right_border >= start:
                heappush(started, right_border)
                break
        overlappings += [len(started)]
        heappush(started, start + length)
    avg_overlapping = sum(overlappings) / float(len(overlappings))    
    #print "avg overlapping", avg_overlapping
    QUERIES_COUNT_SPEC = 1000000
    query_len_mean = 100
    lengths = normal(query_len_mean, len_stdev, QUERIES_COUNT_SPEC)
    queries = []
    for point_index in xrange(QUERIES_COUNT_SPEC):
        start = random.random() * (MAX - MIN) + MIN
        queries += [(start, lengths[point_index])]
    save_dataset("datasets/chi_time_mem_1M_100_1M_100.txt", intervals, queries)


if 0:
    # avg_overlapping
    print ".. datasets for query_time = f(average_overlapping)"
    create_path_silent("datasets/avg_overlapping")
    clean_path("datasets/avg_overlapping")
    queries = []
    for query_index in xrange(QUERIES_COUNT):
        start = random.random() * (MAX - MIN) + MIN
        length = 100
        queries += [(start, length)]
    
    len_mean = 1
    max_len = 100000
    DATASETS_COUNT = 30
    factor = math.exp(math.log(max_len / float(len_mean) ) / (DATASETS_COUNT - 1))
    while len_mean <= 100000:
        if 1:
            intervals = []
            lengths = [length >=0 and length or 0.0 for length in normal(len_mean, len_mean / 20.0, POINTS_COUNT)]
            if len_mean == 1: #here we want overlapping to be zero
                lengths = [0 for l in lengths]
            for interval_index in xrange(POINTS_COUNT):
                start = random.random() * (MAX - MIN) + MIN
                length = lengths[interval_index]
                intervals += [(start, length)]
            intervals.sort()
            overlappings = []
            started = []
            for start, length in intervals:
                while started:
                    right_border = heappop(started)
                    if right_border >= start:
                        heappush(started, right_border)
                        break
                overlappings += [len(started)]
                heappush(started, start + length)
            avg_overlapping = sum(overlappings) / float(len(overlappings))
            save_dataset("datasets/avg_overlapping/%f.txt" % (avg_overlapping), intervals, queries)
        len_mean = math.ceil(len_mean * factor)


if 0:
    # avg_overlapping standard deviation
    print ".. datasets for query_time = f(average_overlapping_stdev)"
    create_path_silent("datasets/avg_overlapping_stdev")
    clean_path("datasets/avg_overlapping_stdev")
    queries = []
    for query_index in xrange(QUERIES_COUNT):
        start = random.random() * (MAX - MIN) + MIN
        length = 100
        queries += [(start, length)]
    len_mean = 10000
    DATASETS_COUNT = 30
    radius = 0
    max_radius = len_mean
    delta = (max_radius - radius) / (float(DATASETS_COUNT - 1))
    for _ in xrange(20):
        if 1:
            intervals = []
            lengths = [length >=0 and length or 0.0 for length in uniform(len_mean - radius, len_mean + radius, POINTS_COUNT)]
            for interval_index in xrange(POINTS_COUNT):
                start = random.random() * (MAX - MIN) + MIN
                length = lengths[interval_index]
                intervals += [(start, length)]
            intervals.sort()
            overlappings = []
            started = []
            for start, length in intervals:
                while started:
                    right_border = heappop(started)
                    if right_border >= start:
                        heappush(started, right_border)
                        break
                overlappings += [len(started)]
                heappush(started, start + length)
            avg_overlapping = sum(overlappings) / float(len(overlappings))
            save_dataset("datasets/avg_overlapping_stdev/%f.txt" % (2 * radius), intervals, queries)
        radius += delta


if 0:
    # different number of intervals
    print ".. datasets for query_time = f(dataset_size)"
    create_path_silent("datasets/intervals_count")
    clean_path("datasets/intervals_count")
    intervals_counts = [10000]
    for _ in xrange(50):
        intervals_counts += [int(1.15 * intervals_counts[-1])]
    max_values = [counts for counts in intervals_counts]
    interval_length = 10
    
    for dataset_index in xrange(len(intervals_counts)):
        intervals_count = intervals_counts[dataset_index]
        MAX = max_values[dataset_index]
        intervals = []
        for _ in xrange(intervals_count):
            start = random.random() * MAX
            intervals += [(start, interval_length)]
        if intervals_count < 10000000:
            intervals.sort()
            overlappings = []
            started = []
            for start, length in intervals:
                while started:
                    right_border = heappop(started)
                    if right_border >= start:
                        heappush(started, right_border)
                        break
                overlappings += [len(started)]
                heappush(started, start + length)
            avg_overlapping = sum(overlappings) / float(len(overlappings))            
        queries = []
        for query_index in xrange(QUERIES_COUNT):
            start = random.random() * MAX
            length = 1000
            queries += [(start, length)]
        save_dataset("datasets/intervals_count/%d.txt" % (intervals_count), intervals, queries)
