from pycarbon.CarbonReader import CarbonReader
from pycarbon.JavaGateWay import JavaGateWay
import sys
import time


def main(argv):
    print("Start")
    start = time.time()
    print(argv)
    gateway = JavaGateWay()
    reader = CarbonReader(gateway.get_java_entry()) \
        .builder() \
        .withFile(argv[1]) \
        .withBatch(1000) \
        .filterEqual("txtContent", "roses") \
        .build()

    i = 0
    while (reader.hasNext()):
        rows = reader.readNextBatchRow()
        for row in rows:
            i = i + 1
            if 0 == i % 1000:
                print(i)
            for column in row:
                column

    print(i)
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
