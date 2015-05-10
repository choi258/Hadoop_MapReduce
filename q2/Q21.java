package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Q21 {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text value_record = new Text();
    private Text zip = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] elements = line.split("::");

      //value-record.set(elements[3]);
      value_record.set("UsersTable");
      zip.set(elements[4]);
      if(elements[3].equals("15")||elements[3].equals("17")){
          output.collect(zip, value_record);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Q21.class);
    conf.setJobName("Users");

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

