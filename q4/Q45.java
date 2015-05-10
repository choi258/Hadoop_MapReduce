package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Q45 {
	static String uid = new String();
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text value_record = new Text();
    

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] tab_delim = line.split("\\t");

      value_record.set(tab_delim[1]);
      //uid.set(tab_delim[0]);
      uid = tab_delim[0];
      output.collect(new Text(uid), value_record);
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

      String TableType = new String();
      String MovieID = new String();
      String UserID = new String();
      String ValueRecord = new String();
      int Rating;
      Hashtable<String, MyObject> map = new Hashtable<String,MyObject>();
      String[] elements_i = new String[2];
      String[] elements_j = new String[2];
      List<String> cache = new ArrayList<String>();

      while(values.hasNext()){
        cache.add(values.next().toString());
      }

      int size = cache.size();
      for(int i = 0; i < size; i++){
        elements_i = cache.get(i).split("::");
        if(elements_i[0].equals("UsersRatingsTable")){
            for(int j = 0; j < size; j++){
                elements_j = cache.get(j).split("::");
                if(elements_j[0].equals("MoviesTable")){
	            Rating = Integer.parseInt(elements_i[1]);
                    MovieID = elements_j[1];
		if(map.containsKey(MovieID)){
    			int tot_rate;
    			int tot_num;
    			double tot_avg;
    			tot_rate = map.get(MovieID).getRating() + Rating;
    			tot_num = map.get(MovieID).getNum() + 1;
    			tot_avg = (double)(tot_rate)/tot_num;
    				map.put(MovieID, new MyObject(tot_rate, tot_num, tot_avg));
		}else{
			map.put(MovieID,new MyObject(Rating,1,Rating));
		}	
                 //   ValueRecord = "MoviesUsersRatingsTable";
                  //output.collect(MovieID, new Text(ValueRecord));
                }
            }
        }
      }
		if(!values.hasNext()){
			ValueRecord = "MoviesUsersRatingsTable";
			for( Iterator iter=map.keySet().iterator(); iter.hasNext(); ) {
				 String id = (String) iter.next();
				 Text title_rate = new Text();
				 MyObject obj  =  map.get( id );
				String f_avg = String.format("%.1f", obj.getAvg());
				 title_rate.set(id+"::"+f_avg);
				 output.collect(title_rate ,new Text(ValueRecord));
			}
		}
    }
  }
	static class MyObject{
		private int rating;
		private int num;
		private double avg;
		public MyObject(int rating, int num, double avg){
			this.rating = rating;
			this.num = num;
			this.avg = avg;
		}
		int getRating(){
			return rating;
		}
		int getNum(){
			return num;
		}
		double getAvg(){
			return avg;
		}
	}

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Q45.class);
    conf.setJobName("UsersRatings");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileInputFormat.addInputPath(conf, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf, new Path(args[2]));

    JobClient.runJob(conf);
  }
}




