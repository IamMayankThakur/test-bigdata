import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class venue {



  public static class venueMapper
       extends Mapper<Object, Text, Text, Text>{
	String myVenue=new String();

	           
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {      
      try{
			String[] fields = value.toString().split(",");


		if(fields[0].equals("info") && fields[1].equals("venue"))
			{
				if(fields[2].startsWith("\""))
					{
					myVenue = fields[2]  + "," + fields[3];
					}				
				else
					myVenue = fields[2];	

			}		

		int extras = Integer.parseInt(fields[8]);
		if(fields[0].equals("ball") && (extras == 0))           
		{
			String myValue = String.valueOf(fields[7]) + "," +String.valueOf(1);	
			context.write(new Text(myVenue+":"+fields[4]) , new Text(myValue));
			
		}
		

		
      }catch(Exception e){
        
       }
    }
  }



  public static class venueReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

float sum_runs = 0;
float sum_del = 0;

for(Text val : values)
		{
			String line = val.toString();
        		String[] field = line.split(",");
			sum_runs += Float.parseFloat(field[0]);
			sum_del += 1.0;		
		}

	float stRate = (((sum_runs)/(sum_del)) * 100);
	String[] arr = (key.toString()).split(":");	
	String myKey = arr[0];	
	
	
	if(sum_del >= 10)	
		context.write(new Text(myKey) , new Text(""+arr[1] + "," + stRate+","+sum_runs));
	}
}

    public static class venueReducer1 extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double maxruns = 0.0;
            double maxrunsf = 0.0;
            String maxname = "";
            for (Text value : values) {
                String temp = value.toString();
                String[] temparr = temp.split(",");
                if (Double.parseDouble(temparr[1]) == maxruns) {

                    if (Double.parseDouble(temparr[2]) > maxrunsf) {
                        maxruns = Double.parseDouble(temparr[1]);
                        maxrunsf = Double.parseDouble(temparr[2]);
                        maxname = temparr[0];
                    }

                } else if (Double.parseDouble(temparr[1]) > maxruns) {
                    maxruns = Double.parseDouble(temparr[1]);
                    maxrunsf = Double.parseDouble(temparr[2]);
                    maxname = temparr[0];
                }
            }
            if ((key.toString()).equals("")) {
            } else {
                context.write(key, new Text(maxname));

            }
        }
    }


  public static void main(String[] args) throws Exception {
	Path out = new Path(args[1]);

    Configuration conf = new Configuration();
conf.set("mapred.textoutputformat.separator", ","); 

    Job job = Job.getInstance(conf, "venue");
    job.setJarByClass(venue.class);
    job.setMapperClass(venueMapper.class);
    
    job.setCombinerClass(venueReducer.class);
    job.setReducerClass(venueReducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, out);
	job.waitForCompletion(true);
	

  }
}








