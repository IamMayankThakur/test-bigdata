//package com.bigdata.ipl_2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BD_186_281_1583 {
	
    // Map function
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    	// word -> final key ; output from map
    	 private Text pair = new Text();
    	 public void map(LongWritable key, Text value, Context context) 
                 throws IOException, InterruptedException {
             // Splitting the line on comma
             String[] lineArr = value.toString().split(",");
             
             if(lineArr[0].equals("ball")) {
            	 String str = lineArr[4].toString() + "," + lineArr[6].toString();
            	 if((lineArr[9].equals("bowled") || lineArr[9].equals("caught") || lineArr[9].equals("lbw")
            			 || lineArr[9].equals("caught and bowled") || lineArr[9].equals("hit wicket") || lineArr[9].equals("stumped") || lineArr[9].equals("obstructing the field"))) {
            		pair.set(str);
            		 context.write(pair, new IntWritable(0));
            	 }
            	 else {
            		 pair.set(str);
            		 context.write(pair, new IntWritable(1));
            	 }
             }
         }      
     }
    
    
    public static ArrayList<kvpair> allkvpairs = new ArrayList<kvpair>();
    
    public static class kvpair implements Comparable<kvpair>{
    	private Text key;
    	private Text value;
    	
    	// constructor
    	public kvpair(Text key, Text value) {
    	   this.key = key;
    	   this.value = value;
    	}
    	
    	//Setters
    	public void setKey(Text key) { this.key = key; }
    	public void setValue(Text value) { this.value = value; }
    	
    	//getters
    	public Text getKey() { return this.key; }
    	public Text getValue() { return this.value; }
    	
    	public IntWritable getSortByWicketsValue() {
    		String[] vals = (this.value).toString().split(",");
    		return new IntWritable(Integer.parseInt(vals[0]));
    	}
    	
    	public IntWritable getSortByBallsValue() {
    		String[] vals = (this.value).toString().split(",");
    		return new IntWritable(-1 *(Integer.parseInt(vals[1])));
    	}
    	
    	@Override
    	public int compareTo(kvpair o) {
    		if (this.getSortByWicketsValue().equals(o.getSortByWicketsValue())) {
    			return (this.getSortByBallsValue().compareTo(o.getSortByBallsValue()));
    		}
    		else {
    			return this.getSortByWicketsValue().compareTo(o.getSortByWicketsValue());    			
    		}
    	}
    	
    }
    
    
    // Reduce function
    public static class MyReducer extends Reducer<Text, IntWritable, Text, Text>{     
    	//final value from reducer
        private IntWritable result1 = new IntWritable();
        private IntWritable result2 = new IntWritable();
        private Text f_result = new Text();
        public void reduce(Text pair, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
        	int ball_total = 0;
        	int wickets_total = 0;
          for (IntWritable val : values) {
        	  ball_total += 1;
        	  if(val.get() == 0) {
        		  wickets_total +=1;
        	  }
          }
          if(ball_total > 5) {
              result1.set(wickets_total);
              result2.set(ball_total);
              String s = result1 + "," + result2;
              f_result.set(s);
//              context.write(pair,f_result);
              allkvpairs.add(new kvpair(new Text(pair),new Text(f_result)));
          }
        }
        
        public void cleanup(Context context) throws IOException,
    	InterruptedException {
        	//Integer count = 0;
        	Collections.sort(allkvpairs, Collections.reverseOrder());
        	for  (kvpair kv : allkvpairs) {
        		//count++;
        		context.write(kv.getKey(), kv.getValue());
        	}
        }        
    }
    
    public static void main(String[] args)  throws Exception{
        Configuration conf = new Configuration();

        conf.set("mapreduce.output.textoutputformat.separator",",");
        Job job = Job.getInstance(conf, "BV");
        job.setJarByClass(BD_186_281_1583.class);
        job.setMapperClass(MyMapper.class);    
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setJarByClass(BatsmanVulnerability.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	
}