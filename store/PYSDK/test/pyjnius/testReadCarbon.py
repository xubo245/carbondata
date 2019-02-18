import time

print("Start")
start = time.time()
import os

os.environ['CLASSPATH'] = '/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/carbondata-sdk.jar'

import jnius_config

jnius_config.add_classpath('/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/carbondata-sdk.jar')

from jnius import autoclass

Stack = autoclass('org.apache.carbondata.sdk.file.CarbonReader')
reader = Stack.builder() \
    .withFile(
    "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers/part-0-184878469873324_batchno0-0-null-184877473198306.carbondata") \
    .build();

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
        row[0];
        row[1];
        row[2];
        row[3];
        row[4];
        j = 0;
        for column in row:
            j = j + 1
            # print("column:" + str(j))
            if (j != 5):
                column
                # print(column)
            else:
                column.tostring()
                # print(column.tostring())
                # print(column[0])
                # for char in column:
                #     print(char, end="")
print(i)
reader.close()

end = time.time()
print("all time is " + str(end - start))
