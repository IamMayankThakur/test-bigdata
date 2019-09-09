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

public class GasWordCount {

	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String venue="";
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

        	String itr = value.toString();
        	String[] Record = itr.split(",");
        	String val = "0";
        	if(Record[0].equals("info")&&Record[1].equals("venue")){
        		venue=Record[2].toString();
        	}
        	if(Record[0].equals("ball")&&Record[8].equals("0"))
        	{

        		context.write(new Text(venue+","+Record[4]), new Text(Record[7]));
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
            if(sum>10){
            context.write(new Text(key.toString().split(",")[0]), new Text(key.toString().split(",")[1]+","+count+","+sum));
}
        }
    }

    public static class IntSumCombiner
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
        	float maxstrike = 0;
        	int maxrun=0;
        	String player="";
        	for(Text value: values)
        	{	int run=Integer.parseInt(value.toString().split(",")[1]);
        		int ball=Integer.parseInt(value.toString().split(",")[2]);
        		float strike=run*100/ball;
        		        		if(strike>maxstrike || (strike==maxstrike&&run>maxrun)){
        		        			maxstrike=strike;
        		        			maxrun=run;
        		        			player=value.toString().split(",")[0];
        		        		}
        	}
        	context.write(new Text(key+","+player),new Text());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "GasWordCount");
        job.setJarByClass(GasWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumCombiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
