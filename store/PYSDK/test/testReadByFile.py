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
        rows = reader.readNextBatchRow()
        # print
        # print
        for row in rows:
            # print("rows")
            i = i + 1
            if 0 == i % 1000:
                print(i)
            # print(i)
            # row[0];
            # row[1];
            # row[2];
            # row[3];
            # row[4];
            # for row in rows:
            # #     # j = j + 1
            # #     # print("column:" + str(j))
            #     row
        # garbage_collect_object(rows)

    print(i)
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
