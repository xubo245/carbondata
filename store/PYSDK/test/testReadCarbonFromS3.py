from pycarbon.CarbonReader import CarbonReader
from pycarbon.JavaGateWay import JavaGateWay
import sys
import time

def main(argv):
    print("Start")
    start = time.time()
    gateway = JavaGateWay()
    reader = CarbonReader(gateway.get_java_entry()) \
        .builder() \
        .withBatch(1000) \
        .withFolder(
        "s3a://carbonsouth/modelarts/flowerscarbon/") \
        .withHadoopConf("fs.s3a.access.key", argv[1]) \
        .withHadoopConf("fs.s3a.secret.key", argv[2]) \
        .withHadoopConf("fs.s3a.endpoint", argv[3]) \
        .build()


    num=0
    build = time.time()
    print("build time:" + str(build - start))
    while (reader.hasNext()):
        beforeBatch = time.time()
        object = reader.readNextBatchRow()
        afterbatch = time.time()
        print("read batch time:" + str(afterbatch - beforeBatch))

        for rows in object:
            num = num + 1
            if(0==(num%1000)):
                print(num)
            rows
            rows[4]
            # print(rows[0])
            #
            # print(rows[1])
            #
            # print(rows[2])
            #
            # print(rows[3])
            #
            # print(rows[4])
            # for row in rows:
            #     row
        readEach = time.time()
        print("read each time:" + str(readEach-afterbatch))
    print(num)
    reader.close()
    end = time.time()
    print("total time:" + str(end - start))
    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
