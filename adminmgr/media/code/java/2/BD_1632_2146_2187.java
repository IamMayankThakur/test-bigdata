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
		int run;
	    	int ball;
	    	String batsmen;
		String bowler;

	    public Sort(int r,int b,String n1,String n2)
	    {

			run = r;
			ball = b;
			bowler = n1;
			batsmen = n2;
	    }
	}

	static class SortCompare implements Comparator<Sort> {
	    
	    public int compare(Sort s1, Sort s2)
	    {
	        int diff_run = s2.run - s1.run;
			if(diff_run > 0 || diff_run < 0)
				return diff_run;
			else
			{
				int diff_ball = s1.ball-s2.ball;
				if(diff_ball == 0)
					return (s1.bowler).compareTo(s2.bowler);
				else
					return diff_ball;
			}
		}
	}

	

	static class BatsmenMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			try{

				int run;

				String[] fields = value.toString().split(",");
				if(fields[0].equals("ball"))
				{
					String my_key = new Text(fields[6]) + "," + new Text(fields[4]);
         				int run1=Integer.parseInt(fields[7]);
					int run2=Integer.parseInt(fields[8]);
					run=run1+run2;
					/*if(fields[9].length() == 2 || fields[9].equals("run out") || fields[9].equals("retired hurt"))
						is_out = 0;
					else
						is_out = 1;*/
					String my_value =String.valueOf(run)+ ","+ String.valueOf(1);
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
			int sum_runs = 0;

			for (Text val : values)
			{
				String line = val.toString();
				String[] field = line.split(",");
				sum_runs += Integer.parseInt(field[0]);
				sum_balls += 1;
			}

			String my_val = String.valueOf(sum_runs) + "," +String.valueOf(sum_balls);

			String line1 = key.toString();
			String[] field1 = line1.split(",");
			String readbowler = field1[0];
			String readbatsmen = field1[1];
			int readrun = sum_runs;
			int readball = sum_balls;

			if(sum_balls > 5)
				records.add(new Sort(readrun,readball,readbowler,readbatsmen));

		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {

		Collections.sort(records, new SortCompare());

		for (Sort res : records)
			context.write(new Text(res.bowler+","+res.batsmen) , new Text(res.run+","+res.ball) );
		}

	}

	
}
