import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
 
public class BD_1632_2146_2187 {



	public static void main(String[] args) throws Exception {
		Path out = new Path(args[1]);

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ","); 

		Job job = Job.getInstance(conf, "Batsmen");
		job.setJarByClass(BD_1632_2146_2187.class);
		job.setMapperClass(BatsmenMapper.class);
		job.setReducerClass(BatsmenReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, out);	
		job.waitForCompletion(true);

	}

	static class Sort {
		int wkt;
	    int ball;
	    String batsmen;
		String bowler;
	   
	    public Sort(int w,int r,String n1,String n2) 
	    {
	        
			wkt = w;	
			ball = r;
			batsmen = n1;
			bowler = n2;
	    }
	}

	static class SortCompare implements Comparator<Sort> {
	    @Override
	    public int compare(Sort s1, Sort s2)
	    {
	        int diff_wkt = s2.wkt - s1.wkt;
			if(diff_wkt > 0 || diff_wkt < 0)		
				return diff_wkt;
			else
			{
				int diff_ball = s1.ball-s2.ball;
				if(diff_ball == 0)
					return (s1.batsmen).compareTo(s2.batsmen);
				else 
					return diff_ball;
			}    
		}
	}

	
    
	static class BatsmenMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 

			try{

				int out = 0;

				String[] fields = value.toString().split(",");
				if(fields[0].equals("ball"))
				{
					String my_key = new Text(fields[4]) + "," + new Text(fields[6]);

					if(fields[9].length() == 2 || fields[9].equals("run out") || fields[9].equals("retired hurt"))		
						out = 0; 
					else
						out = 1;
					String my_value = String.valueOf(out) + ","+ String.valueOf(1);
					context.write(new Text(my_key) , new Text(my_value));
				}

			}
			catch(Exception e){}
		}
	}

	static ArrayList<Sort> records = new ArrayList<Sort>();
 	static class BatsmenReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

			int sum_balls = 0;
			int sum_out = 0;

			for (Text val : values) 
			{
				String line = val.toString();
				String[] field = line.split(",");
				sum_out += Integer.parseInt(field[0]);	
				sum_balls += 1;
			}

			String my_val = String.valueOf(sum_out) + "," +String.valueOf(sum_balls);

			String line1 = key.toString();
			String[] field1 = line1.split(",");
			String readbatsmen = field1[0];	
			String readbowler = field1[1];
			int readwkt = sum_out;             
			int readball = sum_balls;

			if(sum_balls > 5)	
				records.add(new Sort(readwkt,readball,readbatsmen,readbowler));	

		}	
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {

		Collections.sort(records, new SortCompare());

		for (Sort res : records)
			context.write(new Text(res.batsmen+","+res.bowler) , new Text(res.wkt+","+res.ball) );
		}

	}


}
