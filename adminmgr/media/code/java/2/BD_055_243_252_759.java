import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BD_055_243_252_759 {

	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

        	String itr = value.toString();
        	String[] Record = itr.split(",");
        	String val = "0";
        	if(Record[0].equals("ball"))
        	{if(Record[9].equals("caught")||Record[9].equals("bowled")||Record[9].equals("lbw")||Record[9].equals("caught and bowled")||Record[9].equals("hit wicket")||Record[9].equals("obstructing the field")||Record[9].equals("stumped")){
        		val="1";	
        	}

        		context.write(new Text(Record[4]+","+Record[6]), new Text(val));
        	}
        }
    }

    // public static class IntSumCombiner
    // extends Reducer<Text,Text,Text,Text> {
    //     private IntWritable result = new IntWritable();

    //     public void reduce(Text key, Iterable<Text> values,
    //                        Context context
    //                        ) throws IOException, InterruptedException {
    //     	int count = 0;
    //     	for(Text value: values)
    //     	{	
    //     		count ++;
    //     	}
    //     	context.write(key, new Text(""+count));
    //     }
    // }
   
    public static class IntSumReducer
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int count = 0;
            int sum=0;
            for(Text value: values)
            {
            	count += Integer.parseInt(value.toString());
            	sum++;
            }
            context.write(key, new Text(""+count+","+sum));

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BD_055_243_252_759");
        job.setJarByClass(GasWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        //job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
