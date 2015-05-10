package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Q43 {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text value_record = new Text();
    private Text uid = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] tab_delim = line.split("\\t");

      //value_record.set(elements[3]);
      value_record.set(tab_delim[1]);
      uid.set(tab_delim[0]);
      output.collect(uid, value_record);
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      Text value_record = new Text();
      String TableType = new String();
      String MovieID = new String();
      String UserID = new String();
      String[] elements_i = new String[2];
      String[] elements_j = new String[3];
      List<String> cache = new ArrayList<String>();

      // store each line of input in a List;
      while(values.hasNext()){
        cache.add(values.next().toString());
      }

      int size = cache.size();
      // outer loop;
      for(int i = 0; i < size; i++){
        elements_i = cache.get(i).split("::");
        if(elements_i[0].equals("UsersTable")){
            // inner loop;
            for(int j = 0; j < size; j++){
                elements_j = cache.get(j).split("::");
                if(elements_j[0].equals("RatingsTable")){
                    MovieID = elements_j[1];
                    value_record.set("UsersRatingsTable::" + elements_j[2]);
                    output.collect(new Text(MovieID), value_record);
                }
            }
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Q43.class);
    conf.setJobName("UsersRatings");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    // NOTE: we will add another input since this job
    // consumes the output of MR1 and MR2;
    FileInputFormat.addInputPath(conf, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf, new Path(args[2]));

    JobClient.runJob(conf);
  }
}



