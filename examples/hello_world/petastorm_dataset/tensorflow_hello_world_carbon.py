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

"""Minimal example of how to read samples from a dataset generated by `generate_hello_world_dataset.py`
using tensorflow."""

from __future__ import print_function

import jnius_config
import tensorflow as tf

from petastorm import make_carbon_reader, make_batch_carbon_reader
from petastorm.tf_utils import tf_tensors, make_petastorm_dataset


def tensorflow_hello_world(dataset_url='file:///tmp/hello_world_dataset_carbon/'):
  # Example: tf_tensors will return tensors with dataset data
  with make_carbon_reader(dataset_url) as reader:
    tensor = tf_tensors(reader)
    with tf.Session() as sess:
      sample = sess.run(tensor)
      print(sample.id)

  with make_carbon_reader(dataset_url) as reader:
    for schema_view in reader:
      print(schema_view.id)

  # Example: use tf.data.Dataset API
  with make_carbon_reader(dataset_url) as reader:
    dataset = make_petastorm_dataset(reader)
    print(dataset.output_classes)
    print(dataset.output_shapes)
    print(dataset.output_types)
    iterator = dataset.make_one_shot_iterator()
    tensor = iterator.get_next()
    with tf.Session() as sess:
      sample = sess.run(tensor)
      print(sample.id)


if __name__ == '__main__':
  jnius_config.set_classpath(
    "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/carbondata-sdk.jar")
  tensorflow_hello_world()
