#  Copyright (c) 2017-2018 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

###
# Adapted to petastorm dataset using original contents from
# https://github.com/tensorflow/tensorflow/blob/master/tensorflow/examples/tutorials/mnist/mnist_softmax.py
###

from __future__ import division, print_function

import os
import shutil
import time
from multiprocessing.pool import ThreadPool

import jnius_config

from obs import ObsClient
from petastorm import make_carbon_reader, make_batch_carbon_reader


# def just_read_obs(dataset_url, key, secret, endpoint):
#     with make_carbon_reader(dataset_url, key=key, secret=secret, endpoint=endpoint, num_epochs=1, workers_count=16) as train_reader:
#         i = 0
#         for schema_view in train_reader:
#             # print(schema_view.imagename)
#             i = i + 1
#         print(i)


def just_read_batch_obs(key, secret, endpoint, bucketname , prefix, download_path):
    path = 'file://'+ download_files_from_obs_concurrently(key, secret, endpoint, bucketname, prefix, download_path)

    with make_batch_carbon_reader(path, key=key, secret=secret, endpoint=endpoint, num_epochs=1, workers_count=16) as train_reader:
        i = 0
        for schema_view in train_reader:
            # print(schema_view.imagename)
            i += len(schema_view.imagename)
        print(i)


def download_files_from_obs(access_key, secret_key, end_point, bucket_name, prefix, download_path):
    obsClient = ObsClient(
        access_key_id=access_key,
        secret_access_key=secret_key,
        server=end_point,
        long_conn_mode=True
    )
    files = list_obs_files(obsClient, bucket_name, prefix)
    numOfFiles = len(files)
    print(numOfFiles)
    num = 0
    for file in files:
        num = num + 1
        obsClient.getObject(bucket_name, file, download_path + file)
    obsClient.close()
    return 'file://'+ download_path + prefix

def download_files_from_obs_concurrently(access_key, secret_key, end_point, bucket_name, prefix, download_path):
    obsClient = ObsClient(
        access_key_id=access_key,
        secret_access_key=secret_key,
        server=end_point,
        long_conn_mode=True
    )
    files = list_obs_files(obsClient, bucket_name, prefix)
    numOfFiles = len(files)

    def download(file):
        i = 0
        obsClient.getObject(bucket_name, file, download_path + file)

    pool = ThreadPool(numOfFiles)
    results = pool.map(download, files)
    pool.close()
    obsClient.close()
    return 'file://'+ download_path + prefix


def list_obs_files(obs_client, bucket_name, prefix):
    files = []

    pageSize = 1000
    index = 1
    nextMarker = None
    while True:
        resp = obs_client.listObjects(bucket_name, prefix=prefix, max_keys=pageSize, marker=nextMarker)
        for content in resp.body.contents:
            # print(content.key)
            files.append(content.key)
        if not resp.body.is_truncated:
            break
        nextMarker = resp.body.next_marker
        index += 1

    return files


def main():
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_181'
    jnius_config.set_classpath(
        "/home/root1/Documents/ab/workspace/historm_xubo/historm/store/sdk/target/carbondata-sdk.jar")
    jnius_config.add_options('-Xrs', '-Xmx6096m')
    # jnius_config.add_options('-Xrs', '-Xmx6096m', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5555')
    print("Start")
    start = time.time()

    key = "GYLW3KJVB7IFHNF5W3JV"
    secret = "6KYRbqkodxzk9xIka743Z5E1Y3nm9KnGsHPuZqPT"
    endpoint = "obs.cn-south-1.myhwclouds.com"

    # just_read_obs("s3a://modelarts-carbon/imageNet_resize/carbon/", key, secret, endpoint)
    just_read_batch_obs(key, secret, endpoint,
                        'carbonsouth', 'modelarts/mini/upload', '/tmp/download/')
    shutil.rmtree('/tmp/download/')

    end = time.time()
    print("all time: " + str(end - start))
    print("Finish")

if __name__ == '__main__':
    main()
