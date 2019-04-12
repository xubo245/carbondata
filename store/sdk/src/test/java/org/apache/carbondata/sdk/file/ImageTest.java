/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.sdk.file;

import junit.framework.TestCase;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageInputStream;
import javax.sound.midi.SysexMessage;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.carbondata.sdk.file.utils.SDKUtil.listFiles;

public class ImageTest extends TestCase {

  @Test
  public void testBinaryWithFilter() throws IOException, InvalidLoadOptionException, InterruptedException {
    String imagePath = "./src/main/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 1;
    String path = "./target/binary";
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image", DataTypes.BINARY);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example").withPageSizeInMb(1)
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        char[] hexValue = null;
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
          hexValue = Hex.encodeHex(originBinary);
        }
        // write data
        writer.write(new String[]{"robot" + (i % 10), String.valueOf(i), String.valueOf(hexValue)});
        bis.close();
      }
      writer.close();
    }

    // Read data with filter
    EqualToExpression equalToExpression = new EqualToExpression(
        new ColumnExpression("name", DataTypes.STRING),
        new LiteralExpression("robot0", DataTypes.STRING));

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .filter(equalToExpression)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[0] + " " + row[2] + " image size:" + outputBinary.length);

      // validate output binary data and origin binary data
      assert (originBinary.length == outputBinary.length);
      for (int j = 0; j < originBinary.length; j++) {
        assert (originBinary[j] == outputBinary[j]);
      }

      // save image, user can compare the save image and original image
      String destString = "./target/binary/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testBinaryWithoutFilter() throws IOException, InvalidLoadOptionException, InterruptedException {

    String imagePath = "./src/main/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 10;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }

    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image", DataTypes.BINARY);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example").withPageSizeInMb(1)
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        char[] hexValue = null;
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
          hexValue = Hex.encodeHex(originBinary);
        }
        // write data
        writer.write(new String[]{"robot" + (i % 10), String.valueOf(i), String.valueOf(hexValue)});
        bis.close();
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[0] + " " + row[2] + " image size:" + outputBinary.length);

      // validate output binary data and origin binary data
      assert (originBinary.length == outputBinary.length);
      for (int j = 0; j < originBinary.length; j++) {
        assert (originBinary[j] == outputBinary[j]);
      }

      // save image, user can compare the save image and original image
      String destString = "./target/binary/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    reader.close();
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("\nFinished");
  }

  @Test
  public void testBinaryWithManyImages() throws IOException, InvalidLoadOptionException, InterruptedException {
    int num = 1;
    String path = "./target/flowers";
    Field[] fields = new Field[5];
    fields[0] = new Field("imageId", DataTypes.INT);
    fields[1] = new Field("imageName", DataTypes.STRING);
    fields[2] = new Field("imageBinary", DataTypes.BINARY);
    fields[3] = new Field("txtName", DataTypes.STRING);
    fields[4] = new Field("txtContent", DataTypes.STRING);

    String imageFolder = "./src/main/resources/image/flowers";

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example").withPageSizeInMb(1)
          .build();
      File file = new File(imageFolder);
      File[] files = file.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name == null) {
            return false;
          }
          return name.endsWith(".jpg");
        }
      });

      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          // read image and encode to Hex
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(files[i]));
          char[] hexValue = null;
          originBinary = new byte[bis.available()];
          while ((bis.read(originBinary)) != -1) {
            hexValue = Hex.encodeHex(originBinary);
          }

          String txtFileName = files[i].getCanonicalPath().split(".jpg")[0] + ".txt";
          BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
          String txtValue = null;
          byte[] txtBinary = null;
          txtBinary = new byte[txtBis.available()];
          while ((txtBis.read(txtBinary)) != -1) {
            txtValue = new String(txtBinary, "UTF-8");
          }
          // write data
          System.out.println(files[i].getCanonicalPath());
          writer.write(new String[]{String.valueOf(i), files[i].getCanonicalPath(), String.valueOf(hexValue),
              txtFileName, txtValue});
          bis.close();
        }
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder()
        .withFolder(path)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[0] + " " + row[2] + " image size:" + outputBinary.length);

      // save image, user can compare the save image and original image
      String destString = "./target/flowers/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testBinaryWithProjectionAndFileLists() throws IOException, InvalidLoadOptionException, InterruptedException {
    int num = 2;
    String path = "./target/flowers2";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[5];
    fields[0] = new Field("imageId", DataTypes.INT);
    fields[1] = new Field("imageName", DataTypes.STRING);
    fields[2] = new Field("imageBinary", DataTypes.BINARY);
    fields[3] = new Field("txtName", DataTypes.STRING);
    fields[4] = new Field("txtContent", DataTypes.STRING);

    String imageFolder = "./src/main/resources/image/flowers";

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example").withPageSizeInMb(1)
          .build();
      File file = new File(imageFolder);
      File[] files = file.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name == null) {
            return false;
          }
          return name.endsWith(".jpg");
        }
      });

      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          // read image and encode to Hex
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(files[i]));
          char[] hexValue = null;
          originBinary = new byte[bis.available()];
          while ((bis.read(originBinary)) != -1) {
            hexValue = Hex.encodeHex(originBinary);
          }

          String txtFileName = files[i].getCanonicalPath().split(".jpg")[0] + ".txt";
          BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
          String txtValue = null;
          byte[] txtBinary = null;
          txtBinary = new byte[txtBis.available()];
          while ((txtBis.read(txtBinary)) != -1) {
            txtValue = new String(txtBinary, "UTF-8");
          }
          // write data
          System.out.println(files[i].getCanonicalPath());
          writer.write(new String[]{String.valueOf(i), files[i].getCanonicalPath(), String.valueOf(hexValue),
              txtFileName, txtValue});
          bis.close();
        }
      }
      writer.close();
    }

    File carbonDataFile = new File(path);
    File[] carbonDataFiles = carbonDataFile.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name == null) {
          return false;
        }
        return name.endsWith(CarbonTablePath.CARBON_DATA_EXT);
      }
    });

    System.out.println(carbonDataFiles[0]);
    List fileLists = new ArrayList();
    fileLists.add(carbonDataFiles[0]);
    fileLists.add(carbonDataFiles[1]);

    Schema schema = CarbonSchemaReader.readSchema(carbonDataFiles[0].getAbsolutePath()).asOriginOrder();
    List projectionLists = new ArrayList();
    projectionLists.add((schema.getFields())[1].getFieldName());
    projectionLists.add((schema.getFields())[2].getFieldName());

    CarbonReader reader = CarbonReader
        .builder()
        .withFileLists(fileLists)
        .projection(projectionLists)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      assertEquals(2, row.length);
      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[0] + " " + row[1] + " image size:" + outputBinary.length);

      // save image, user can compare the save image and original image
      String destString = "./target/flowers2/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    assert (4 == i);
    System.out.println("\nFinished");
    reader.close();

    CarbonReader reader2 = CarbonReader
        .builder()
        .withFile(carbonDataFiles[0].toString())
        .build();

    System.out.println("\nData2:");
    i = 0;
    while (i < 20 && reader2.hasNext()) {
      Object[] row = (Object[]) reader2.readNextRow();
      assertEquals(5, row.length);

      assert (null != row[0].toString());
      assert (null != row[0].toString());
      assert (null != row[0].toString());
      byte[] outputBinary = (byte[]) row[1];

      String txt = row[2].toString();
      System.out.println(row[0] + " " + row[2] +
          " image size:" + outputBinary.length + " txt size:" + txt.length());

      // save image, user can compare the save image and original image
      String destString = "./target/flowers2/image" + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader2.close();
  }

  @Test
  public void testWithoutTablePath() throws IOException, InterruptedException {
    try {
      CarbonReader
          .builder()
          .build();
      assert (false);
    } catch (IllegalArgumentException e) {
      assert (e.getMessage().equalsIgnoreCase("Please set table path first."));
      assert (true);
    }
  }

  @Test
  public void testWriteWithByteArrayDataType() throws IOException, InvalidLoadOptionException, InterruptedException {
    String imagePath = "./src/main/resources/image/carbondatalogo.jpg";
    int num = 1;
    int rows = 10;
    String path = "./target/binary";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Field[] fields = new Field[5];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("image1", DataTypes.BINARY);
    fields[3] = new Field("image2", DataTypes.BINARY);
    fields[4] = new Field("image3", DataTypes.BINARY);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example").withPageSizeInMb(1)
          .build();

      for (int i = 0; i < rows; i++) {
        // read image and encode to Hex
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(imagePath));
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
        }
        // write data
        writer.write(new Object[]{"robot" + (i % 10), i, originBinary, originBinary, originBinary});
        bis.close();
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      byte[] outputBinary2 = (byte[]) row[2];
      byte[] outputBinary3 = (byte[]) row[3];
      System.out.println(row[0] + " " + row[1] + " image1 size:" + outputBinary.length
          + " image2 size:" + outputBinary2.length + " image3 size:" + outputBinary3.length);

      for (int k = 0; k < 3; k++) {

        byte[] originBinaryTemp = (byte[]) row[1 + k];
        // validate output binary data and origin binary data
        assert (originBinaryTemp.length == outputBinary.length);
        for (int j = 0; j < originBinaryTemp.length; j++) {
          assert (originBinaryTemp[j] == outputBinary[j]);
        }

        // save image, user can compare the save image and original image
        String destString = "./target/binary/image" + k + "_" + i + ".jpg";
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
        bos.write(originBinaryTemp);
        bos.close();
      }
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  // TODO
  public void testWriteTwoImageColumn() throws Exception {
    String imagePath = "./src/main/resources/image/vocForSegmentationClass";
    String path = "./target/vocForSegmentationClass";
    int num = 1;
    int rows = 10;
    Field[] fields = new Field[4];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("rawImage", DataTypes.BINARY);
    fields[3] = new Field("segmentationClass", DataTypes.BINARY);

    byte[] originBinary = null;
    byte[] originBinary2 = null;

    Object[] files = listFiles(imagePath, ".jpg").toArray();
    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("SDKS3Example").withPageSizeInMb(1)
          .build();

      for (int i = 0; i < files.length; i++) {
        // read image and encode to Hex
        String filePath = (String) files[i];
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filePath));
        originBinary = new byte[bis.available()];
        while ((bis.read(originBinary)) != -1) {
        }

        BufferedInputStream bis2 = new BufferedInputStream(new FileInputStream(filePath.replace(".jpg", ".png")));
        originBinary2 = new byte[bis2.available()];
        while ((bis2.read(originBinary2)) != -1) {
        }

        // write data
        writer.write(new Object[]{"robot" + (i % 10), i, originBinary, originBinary2});
        bis.close();
        bis2.close();
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      byte[] outputBinary2 = (byte[]) row[2];
      System.out.println(row[0] + " " + row[3] + " image1 size:" + outputBinary.length
          + " image2 size:" + outputBinary2.length);

      for (int k = 0; k < 2; k++) {

        byte[] originBinaryTemp = (byte[]) row[1 + k];

        // save image, user can compare the save image and original image
        String destString = null;
        if (k == 0) {
          destString = "./target/vocForSegmentationClass/image" + k + "_" + i + ".jpg";
        } else {
          destString = "./target/vocForSegmentationClass/image" + k + "_" + i + ".png";
        }
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
        bos.write(originBinaryTemp);
        bos.close();
      }
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

  @Test
  public void testWriteWithByteArrayDataTypeAndManyImagesTxt()
      throws Exception {
    long startWrite = System.nanoTime();
    String sourceImageFolder = "./src/main/resources/image/flowers";
    String outputPath = "./target/flowers";
    String preDestPath = "./target/flowers/image";
    String sufAnnotation = ".txt";
    writeAndRead(sourceImageFolder, outputPath, preDestPath, sufAnnotation, ".jpg");
    long endWrite = System.nanoTime();
    System.out.println("write time is " + (endWrite - startWrite) / 1000000000.0 + "s");
  }

  @Test
  public void testWriteWithByteArrayDataTypeAndManyImagesXml()
      throws Exception {
    long startWrite = System.nanoTime();
    String sourceImageFolder = "./src/main/resources/image/voc";

    String outputPath = "./target/voc";
    String preDestPath = "./target/voc/image";
    String sufAnnotation = ".xml";
    writeAndRead(sourceImageFolder, outputPath, preDestPath, sufAnnotation, ".jpg");
    long endWrite = System.nanoTime();
    System.out.println("write time is " + (endWrite - startWrite) / 1000000000.0 + "s");
    ReadPerformance();
  }

  public void writeAndRead(String sourceImageFolder, String outputPath,
                           String preDestPath, String sufAnnotation, final String sufImage)
      throws Exception {
    int num = 1;
    Field[] fields = new Field[5];
    fields[0] = new Field("imageId", DataTypes.INT);
    fields[1] = new Field("imageName", DataTypes.STRING);
    fields[2] = new Field("imageBinary", DataTypes.BINARY);
    fields[3] = new Field("txtName", DataTypes.STRING);
    fields[4] = new Field("txtContent", DataTypes.STRING);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {
      CarbonWriter writer = CarbonWriter
          .builder()
          .outputPath(outputPath)
          .withCsvInput(new Schema(fields))
          .withBlockSize(256)
          .writtenBy("SDKS3Example").withPageSizeInMb(1)
          .build();
      Object[] files = listFiles(sourceImageFolder, sufImage).toArray();

      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          // read image and encode to Hex
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File((String) files[i])));
          originBinary = new byte[bis.available()];
          while ((bis.read(originBinary)) != -1) {
          }

          String txtFileName = ((String) files[i]).split(sufImage)[0] + sufAnnotation;
          BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
          String txtValue = null;
          byte[] txtBinary = null;
          txtBinary = new byte[txtBis.available()];
          while ((txtBis.read(txtBinary)) != -1) {
            txtValue = new String(txtBinary, "UTF-8");
          }
          // write data
          System.out.println(files[i]);
          writer.write(new Object[]{i, (String) files[i], originBinary,
              txtFileName, txtValue});
          bis.close();
        }
      }
      writer.close();
    }

    CarbonReader reader = CarbonReader
        .builder()
        .withFolder(outputPath)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[0] + " " + row[2] + " image size:" + outputBinary.length);

      // save image, user can compare the save image and original image
      String destString = preDestPath + i + ".jpg";
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("number of reading: " + i);
    System.out.println("\nFinished");
    reader.close();
  }

  public void ReadPerformance() throws Exception {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "2048");

    long start = System.nanoTime();
    int i = 0;
    String path = "./target/voc";
    CarbonReader reader2 = CarbonReader
        .builder()
        .withFolder(path)
        .withBatch(1000)
        .build();

    System.out.println("\nData2:");
    i = 0;
    while (reader2.hasNext()) {
      Object[] rows = reader2.readNextBatchRow();

      for (int j = 0; j < rows.length; j++) {
        Object[] row = (Object[]) rows[j];
        i++;
        if (0 == i % 1000) {
          System.out.println(i);
        }
        for (int k = 0; k < row.length; k++) {
          Object column = row[k];
        }
      }
    }

    System.out.println(i);
    reader2.close();
    long end = System.nanoTime();
    System.out.println("all time is " + (end - start) / 1000000000.0);
    System.out.println("\nFinished");
  }

  @Test
  public void testWriteWithByteArrayDataTypeAndManyImagesTxt3()
      throws Exception {
    String sourceImageFolder = "./src/main/resources/image/flowers";
    String outputPath = "./target/flowers2";
    String preDestPath = "./target/flowers2/image";
    String sufAnnotation = ".txt";
    try {
      FileUtils.deleteDirectory(new File(outputPath));
    } catch (IOException e) {
      e.printStackTrace();
    }
    writeAndReadWithHWD(sourceImageFolder, outputPath, preDestPath, sufAnnotation, ".jpg", 2000);
    try {
      FileUtils.deleteDirectory(new File(outputPath));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testNumberOfFiles() throws Exception {
    String sourceImageFolder = "./src/main/resources/image/flowers";
    List result = listFiles(sourceImageFolder, ".jpg");
    assertEquals(3, result.size());
  }

  public void writeAndReadWithHWD(String sourceImageFolder, String outputPath, String preDestPath,
                                  String sufAnnotation, final String sufImage, int numToWrite)
      throws Exception {
    int num = 1;
    Field[] fields = new Field[7];
    fields[0] = new Field("height", DataTypes.INT);
    fields[1] = new Field("width", DataTypes.INT);
    fields[2] = new Field("depth", DataTypes.INT);
    fields[3] = new Field("imageName", DataTypes.STRING);
    fields[4] = new Field("imageBinary", DataTypes.BINARY);
    fields[5] = new Field("txtName", DataTypes.STRING);
    fields[6] = new Field("txtContent", DataTypes.STRING);

    byte[] originBinary = null;

    // read and write image data
    for (int j = 0; j < num; j++) {

      Object[] files = listFiles(sourceImageFolder, sufImage).toArray();

      int index = 0;

      if (null != files) {
        CarbonWriter writer = CarbonWriter
            .builder()
            .outputPath(outputPath)
            .withCsvInput(new Schema(fields))
            .withBlockSize(256)
            .writtenBy("SDKS3Example").withPageSizeInMb(1)
            .build();

        for (int i = 0; i < files.length; i++) {
          if (0 == index % numToWrite) {
            writer.close();
            writer = CarbonWriter
                .builder()
                .outputPath(outputPath)
                .withCsvInput(new Schema(fields))
                .withBlockSize(256)
                .writtenBy("SDKS3Example").withPageSizeInMb(1)
                .build();
          }
          index++;

          // read image and encode to Hex
          File file = new File((String) files[i]);
          System.out.println(file.getCanonicalPath());
          BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
          int depth = 0;
          boolean isGray;
          boolean hasAlpha;
          BufferedImage bufferedImage = null;
          try {
            bufferedImage = ImageIO.read(file);
            isGray = bufferedImage.getColorModel().getColorSpace().getType() == ColorSpace.TYPE_GRAY;
            hasAlpha = bufferedImage.getColorModel().hasAlpha();

            if (isGray) {
              depth = 1;
            } else if (hasAlpha) {
              depth = 4;
            } else {
              depth = 3;
            }

          } catch (Exception e) {
            e.printStackTrace();
            System.out.println(i);
            ImageInputStream stream = new FileImageInputStream(new File(file.getCanonicalPath()));
            Iterator<ImageReader> iter = ImageIO.getImageReaders(stream);

            Exception lastException = null;
            while (iter.hasNext()) {
              ImageReader reader = null;
              try {
                reader = (ImageReader) iter.next();
                ImageReadParam param = reader.getDefaultReadParam();
                reader.setInput(stream, true, true);
                Iterator<ImageTypeSpecifier> imageTypes = reader.getImageTypes(0);

                while (imageTypes.hasNext()) {
                  ImageTypeSpecifier imageTypeSpecifier = imageTypes.next();
                  System.out.println(imageTypeSpecifier.getColorModel().getColorSpace().getType());
                  int bufferedImageType = imageTypeSpecifier.getBufferedImageType();
                  if (bufferedImageType == BufferedImage.TYPE_BYTE_GRAY) {
                    param.setDestinationType(imageTypeSpecifier);
                    break;
                  }
                }
                bufferedImage = reader.read(0, param);
                isGray = bufferedImage.getColorModel().getColorSpace().getType() == ColorSpace.TYPE_GRAY;
                hasAlpha = bufferedImage.getColorModel().hasAlpha();

                if (isGray) {
                  depth = 1;
                } else if (hasAlpha) {
                  depth = 4;
                } else {
                  depth = 3;
                }
                if (null != bufferedImage) break;
              } catch (Exception e2) {
                lastException = e2;
              } finally {
                if (null != reader) reader.dispose();
              }
            }
            // If you don't have an image at the end of all readers
            if (null == bufferedImage) {
              if (null != lastException) {
                throw lastException;
              }
            }
          } finally {
            originBinary = new byte[bis.available()];
            while ((bis.read(originBinary)) != -1) {
            }

            String txtFileName = file.getCanonicalPath().split(sufImage)[0] + sufAnnotation;
            BufferedInputStream txtBis = new BufferedInputStream(new FileInputStream(txtFileName));
            String txtValue = null;
            byte[] txtBinary = null;
            txtBinary = new byte[txtBis.available()];
            while ((txtBis.read(txtBinary)) != -1) {
              txtValue = new String(txtBinary, "UTF-8");
            }
            // write data
            writer.write(new Object[]{bufferedImage.getHeight(), bufferedImage.getWidth(), depth, file.getCanonicalPath(), originBinary,
                txtFileName, txtValue.replace("\n", "")});
            bis.close();
          }
        }
        writer.close();
      }
    }

    CarbonReader reader = CarbonReader
        .builder()
        .withFolder(outputPath)
        .build();

    System.out.println("\nData:");
    int i = 0;
    while (i < 20 && reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();

      byte[] outputBinary = (byte[]) row[1];
      System.out.println(row[2] + " " + row[3] + " " + row[4] + " " + row[5] + " image size:" + outputBinary.length + " " + row[0]);

      // save image, user can compare the save image and original image
      String destString = preDestPath + i + sufImage;
      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destString));
      bos.write(outputBinary);
      bos.close();
      i++;
    }
    System.out.println("\nFinished");
    reader.close();
  }

}
