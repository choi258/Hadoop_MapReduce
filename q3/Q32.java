package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Q32 {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text value_record = new Text();
    private Text uid = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] elements = line.split("::");

      value_record.set("RatingsTable::" + elements[1]);
      uid.set(elements[0]);
      if(elements[2].equals("5")){
          output.collect(uid, value_record);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Q32.class);
    conf.setJobName("Ratings");

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    //conf.setReducerClass(Reduce.class);
    conf.setNumReduceTasks(0);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}


