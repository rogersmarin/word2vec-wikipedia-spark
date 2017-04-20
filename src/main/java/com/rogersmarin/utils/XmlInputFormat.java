
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rogersmarin.utils;

import org.apache.commons.io.Charsets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {

  private static final Logger log = LoggerFactory.getLogger(XmlInputFormat.class);

  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
      return new XmlRecordReader();
  }

  /**
   * XMLRecordReader class to read through a given xml document to output xml blocks as records as specified
   * by the start tag and end tag
   * 
   */
  public static class XmlRecordReader extends RecordReader<LongWritable, Text> {

    private byte[] startTag;
    private byte[] endTag;
    private byte[] space;
    private byte[] angleBracket;
    private long start;
    private long end;
    private DataInputStream fsin = null;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable currentKey;
    private Text currentValue;
    private byte[] currentStartTag;
    private long recordStartPos;
    private long pos;

    @Override
    public void initialize(InputSplit input, TaskAttemptContext context) throws IOException {
      Configuration conf = context.getConfiguration();
      FileSplit split = (FileSplit) input;
      startTag = conf.get(START_TAG_KEY).getBytes(Charsets.UTF_8);
      endTag = conf.get(END_TAG_KEY).getBytes(Charsets.UTF_8);
      space = " ".getBytes(Charsets.UTF_8);
      angleBracket = ">".getBytes(Charsets.UTF_8);
      // open the file and seek to the start of the split
      start = split.getStart();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(conf);

      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
      CompressionCodec codec = compressionCodecs.getCodec(file);

      if (codec != null) {
        fsin = new DataInputStream(codec.createInputStream(fs.open(file)));
        end = Long.MAX_VALUE;
      } else {
        FSDataInputStream fileIn = fs.open(file);
        fileIn.seek(start);
        fsin = fileIn;
        end = start + split.getLength();
      }
      
      recordStartPos = start;

      // Because input streams of gzipped files are not seekable, we need to keep track of bytes
      // consumed ourselves.
      pos = start;
    }

    private boolean next(LongWritable key, Text value) throws IOException {
      if (pos < end) {
        if (readUntilStartElement()) {
          recordStartPos = pos - startTag.length;
          try {
            buffer.write(currentStartTag);
            if (readUntilEndElement()) {
              key.set(recordStartPos);
              value.set(buffer.getData(), 0, buffer.getLength());
              return true;
            } else {
              return false;
            }
          } finally {
            buffer.reset();
          }
        } else {
          return false;
        }
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      fsin.close();
    }

    @Override
    public float getProgress() throws IOException {
      return (pos - start) / (float) (end - start);
    }



    private Boolean readUntilStartElement()  throws IOException{
      currentStartTag = startTag;
      int i = 0;
      while (true) {
        int b = fsin.read();
        pos++;
        if (b == -1 || (i == 0 && pos > end)) {
          // End of file or end of split.
          return false;
        } else {
          if (b == startTag[i]) {
            if (i >= startTag.length - 1) {
              // Found start tag.
              return true;
            } else {
              // In start tag.
              i += 1;
            }
          } else {
            if (i == (startTag.length - angleBracket.length) && checkAttributes(b)) {
              // Found start tag with attributes.
              return true;
            } else {
              // Not in start tag.
              i = 0;
            }
          }
        }
      }
    }

    private Boolean readUntilEndElement()  throws IOException{
      int si = 0;
      int ei = 0;
      int depth = 0;
      while (true) {
        int b = fsin.read();
        pos++;
        if (b == -1) {
          // End of file (ignore end of split).
          return false;
        } else {
          buffer.write(b);
          if (b == startTag[si] && b == endTag[ei]) {
            // In start tag or end tag.
            si += 1;
            ei += 1;
          } else if (b == startTag[si]) {
            if (si >= startTag.length - 1) {
              // Found start tag.
              si = 0;
              ei = 0;
              depth += 1;
            } else {
              // In start tag.
              si += 1;
              ei = 0;
            }
          } else if (b == endTag[ei]) {
            if (ei >= endTag.length - 1) {
              if (depth == 0) {
                // Found closing end tag.
                return true;
              } else {
                // Found nested end tag.
                si = 0;
                ei = 0;
                depth -= 1;
              }
            } else {
              // In end tag.
              si = 0;
              ei += 1;
            }
          } else {
            // Not in start tag or end tag.
            si = 0;
            ei = 0;
          }
        }
      }
    }

    private Boolean checkAttributes(int current) throws IOException {
      int len = 0;
      int b = current;
      while (len < space.length && b == space[len]) {
        len += 1;
        if (len >= space.length) {
          int totalLength = (startTag.length - angleBracket.length);
          byte[] tag = new byte[totalLength];
          for(int i = 0; i <(startTag.length - angleBracket.length); i++){
            tag[i] = startTag[i];
          }
          currentStartTag = ArrayUtils.addAll(tag, space);
          return true;
        }
        b = fsin.read();
        pos++;
      }
      return false;
    }


    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      currentKey = new LongWritable();
      currentValue = new Text();
      return next(currentKey, currentValue);
    }
  }
}

