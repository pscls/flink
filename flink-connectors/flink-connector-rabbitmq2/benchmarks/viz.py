import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from os import listdir
from os.path import isfile, join

def throughput():
    print("** DEFAULT STATS **")
    print("AVERAGE: " + str(len(values) / (values[-1]) * 1000))

def windowThroughput():
    window_length = 8000 # 8second as in paper
    window_slide = 4000 #4sec as in paper

    window_start = 0
    window_end = window_start + window_length

    throughputs = []

    while (window_start < values[-1]):
        values_in_window = [v for v in values if window_start <= v and v < window_end]
        throughputs.append(len(values_in_window) / (window_length/1000))
        window_start += window_slide
        window_end += window_slide

    throughputs = throughputs[:-1]

    throughputs.sort()

    print("** WINDOW STATS **")
    print("#windows: " + str(len(throughputs)))
    print("MIN: " + str(min(throughputs)))
    print("MAX: " + str(max(throughputs)))
    print("AVERAGE: " + str(sum(throughputs)/len(throughputs)))
    print("MEDIAN: " + str(throughputs[int(len(throughputs)*0.5)]))
    print("Perc.9: " + str(throughputs[int(len(throughputs)*0.9)]))
    print("Perc.95: " + str(throughputs[int(len(throughputs)*0.95)]))
    print("Perc.999: " + str(throughputs[int(len(throughputs)*0.99)]))


def viz():
    mapped_ms = [v - values[0] for v in values]
    mapped_s = [int(v / 1000)  for v in mapped_ms]
    throughput = [0] * (mapped_s[-1] + 1)

    for v in mapped_s:
        throughput[v] += 1

    # remove last entry == last second
    # the last second probably contains less messages and thus uglifies our viz
    throughput = throughput[:-1]

    # print(mapped_s[:20])
    plt.plot(throughput)
    plt.show()


def readFile(filename):
    file1 = open(filename, 'r')
    values = file1.readlines()
    return values

def readDir(dirname):
    onlyfiles = [join(dirname, f) for f in listdir(dirname) if isfile(join(dirname, f))]
    values = []
    for filename in onlyfiles:
        values += readFile(filename)
    values.sort()
    return values


# values = readFile('temp.txt')
values = readDir('atmostThroughputBenchmarkSim')

values = [int(v) for v in values]
values = [v - values[0] for v in values]

print("#Messages: " + str(len(values)))
throughput()
windowThroughput()
viz()
