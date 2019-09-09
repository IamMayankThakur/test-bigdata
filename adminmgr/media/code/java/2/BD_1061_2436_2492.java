package pesit.hadoop.mr.ipl.task.three;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * TASK-THREE
 * 
 * Problem Statement:  
 * Bowler Vulnerability to a Batsman. 
*/
public class BD_1061_2436_2492 {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		if (args.length < 2) {
			System.err.println("Usage: task3 <in> [<in>...] out>");
			System.exit(-1);
		}

		Job job = new Job(conf, "task3");
		job.setJarByClass(BD_1061_2436_2492.class);
		job.setMapperClass(MapperIPLBowlVulnerToBats.class);
		job.setReducerClass(ReduceIPLBowlVulnerToBats.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}


class MapperIPLBowlVulnerToBats extends Mapper<LongWritable, Text, Text, IntWritable> {

	// Declaration
	String chooseBallBat = "";
	int inningsNum = 0;
	double overBallNum = 0.0;
	String teamName = "";

	String batsmanOnStrike = "";
	String batsmanNonStrike = "";
	String bowler = "";
	int scoredRuns = 0;
	int extraRuns = 0;
	String dismissalType = "";
	String dismissedBatsman = "";
	String dismissedOrNot = "0";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",");

		while (tokenizer.hasMoreTokens()) {
			
			// Initialization
			chooseBallBat = "";
			inningsNum = 0;
			overBallNum = 0.0;
			teamName = "";
			
			batsmanOnStrike = "";
			batsmanNonStrike = "";
			bowler = "";
			scoredRuns = 0;
			extraRuns = 0;
			dismissalType = "";
			dismissedBatsman = "";
			dismissedOrNot = "0";

			// Manipulation
			chooseBallBat = tokenizer.nextToken().trim();

			if (chooseBallBat.equalsIgnoreCase("ball")) {
				
				inningsNum = Integer.parseInt(tokenizer.nextToken());
				overBallNum = Double.parseDouble(tokenizer.nextToken());
				teamName = tokenizer.nextToken();

				batsmanOnStrike = tokenizer.nextToken().trim();
				batsmanNonStrike = tokenizer.nextToken().trim();
				bowler = tokenizer.nextToken().trim();
				scoredRuns = Integer.parseInt(tokenizer.nextToken().trim());
				extraRuns = Integer.parseInt(tokenizer.nextToken().trim());
				dismissalType = tokenizer.nextToken().trim();
				dismissedBatsman = tokenizer.nextToken().trim();

				System.out.println("value = " + value);
				
				String intermediateRes = bowler + "," + batsmanOnStrike + "," + scoredRuns;
				System.out.println(intermediateRes);
				System.out.println();
				
				// Set values to Mapper output.
				value.set(intermediateRes);
				context.write(value, new IntWritable(1));
			}
		}
	}
}


class ReduceIPLBowlVulnerToBats extends Reducer<Text, IntWritable, Text, NullWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable x : values) {
			sum += x.get();
		}

		String finalKey = key.toString() + "," + sum;
		Text textKey = new Text();
		textKey.set(finalKey);
		
		context.write(textKey, null);
		System.out.println("Reduce phase done for : " + key + " and sum = \t\t" + sum + " and textKey = " + textKey);
	}
}
