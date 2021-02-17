import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from os import listdir
from os.path import isfile, join

def viz():
    plt.plot(values)
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
values = readDir('eventLatency')
print(values[:10])
print(values[-10:])
values = [v.split("-") for v in values]
values = [int(v[1])-int(v[0]) for v in values]
# values = [v - values[0] for v in values]

print("#Messages: " + str(len(values)))
print("Average: " + str(sum(values)/len(values)))
viz()
