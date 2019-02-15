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
        .build()

    i = 0
    while (reader.hasNext()):
        object = reader.readNextBatchRow()
        # print
        # print
        for rows in object:
            # print("rows")
            if 0 == i % 1000:
                print(i)
            i = i + 1
            # print(i)
            j = 0;
            for row in rows:
                j = j + 1
                # print("column:" + str(j))
                row

    print(i)
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
