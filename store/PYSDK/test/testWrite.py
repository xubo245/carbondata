from pycarbon.CarbonWriter import CarbonWriter
from pycarbon.CarbonReader import CarbonReader
from pycarbon.JavaGateWay import JavaGateWay
import sys
import time


def main(argv):
    print("Start")
    start = time.time()
    print(argv)
    gateway = JavaGateWay()
    jsonSchema = "[{stringField:string},{shortField:short},{intField:int}]";
    path = "./store/PYSDK/data/writeCarbon"
    writer = CarbonWriter(gateway.get_java_entry()) \
        .builder() \
        .outputPath(path) \
        .withCsvInput(jsonSchema) \
        .writtenBy("pycarbon") \
        .build()

    for i in range(0, 10):
        string_class = gateway.get_jvm().String
        string_array = gateway.get_gate_way().new_array(string_class, 3)
        string_array[0] = "pycarbon"
        string_array[1] = str(i)
        string_array[2] = str(i * 10)
        writer.write(string_array)
    writer.close()

    reader = CarbonReader(gateway.get_java_entry()) \
        .builder() \
        .withFolder(path) \
        .withBatch(1000) \
        .build()

    i = 0
    while (reader.hasNext()):
        rows = reader.readNextBatchRow()
        for row in rows:
            i = i + 1
            print()
            if 1 == i % 10:
                print(str(i) + " to " + str(i + 9) + ":")
            for column in row:
                print(column, end="\t")

    print()
    print("number of rows by read: " + str(i))
    reader.close()

    end = time.time()
    print("all time is " + str(end - start))
    print("Finish")


if __name__ == '__main__':
    main(sys.argv)
