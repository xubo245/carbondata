import os
import time
import cv2
import threading

import tensorflow as tf

import multiprocessing

process_num = 50

srcdir="/huawei/data/imageNet/rawImage/wholesize"
destdir="/huawei/data/imageNet/rawImage/whole_process_3"

print("list images")
images = []
for dir_path, _, files in os.walk(srcdir):
    for file in files:
        if "JPEG" in file:
            images.append(os.path.join(dir_path, file))

image_num = len(images)
print(image_num)

instances_per_shard = (int)(image_num / process_num)

class ClockProcess(multiprocessing.Process):
    def __init__(self, begin, end):
        multiprocessing.Process.__init__(self)
        self.begin = begin
        self.end = end

    def run(self):
        for i in range(self.begin, self.end):
            image_raw_data = tf.gfile.FastGFile(images[i], 'rb').read()
            image_info = cv2.imread(images[i])

            height, width, depth = image_info.shape

            size = tf.cast(160, tf.float32)
            parsed_height = tf.cast(height, tf.float32)
            parsed_width = tf.cast(width, tf.float32)
            ratio = size / tf.minimum(parsed_width, parsed_height)

            resize_height = tf.cast(tf.cond(tf.greater(parsed_width, parsed_height),
                                            lambda : size,
                                            lambda : ratio * parsed_height), tf.int32)

            resize_width = tf.cast(tf.cond(tf.greater(parsed_width, parsed_height),
                                            lambda : ratio * parsed_width,
                                            lambda : size), tf.int32)

            with tf.Session() as sess:
                img_data = tf.image.decode_jpeg(image_raw_data, channels=3)

                img_data = tf.image.convert_image_dtype(img_data, dtype=tf.float32)

                img_data.set_shape([None, None, 3])

                resized = tf.image.resize_images(img_data, (resize_height, resize_width))

                resized = tf.image.convert_image_dtype(resized, dtype=tf.uint8)

                encoded_image = tf.image.encode_jpeg(resized)
 
                fileext = os.path.basename(images[i])
                dirpath = fileext.split("_")[0]
                filename = fileext.split(".")[0]

                if os.path.exists(destdir + "/" + dirpath) == False:
                    os.mkdir(destdir + "/" + dirpath)

                newfilename = destdir + "/" + dirpath + "/" + filename + ".JPEG"
                with tf.gfile.GFile(newfilename, 'wb') as f:
                    f.write(encoded_image.eval())

                #image_new_info = cv2.imread(newfilename)
                #height, width, depth = image_new_info.shape
            tf.reset_default_graph()    

if __name__ == '__main__':
    for i in range(process_num):
        if i == process_num-1:
            p = ClockProcess(i*instances_per_shard, image_num)
            p.start()
        else:
            p = ClockProcess(i*instances_per_shard, i*instances_per_shard + instances_per_shard)
            p.start()
