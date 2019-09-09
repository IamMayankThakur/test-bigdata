import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class WordCount_1 {
 
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
 
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
 
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	String line=value.toString();
	String array[]=line.split(",");
	if(array[0].equals("ball"))
	{
		
		word.set(array[6]+","+array[4]);
		//context.write(word,one);
		context.write(word,new Text(array[7]+","+"1"+","+array[8]));
	}
        
    }
  }
 
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();
     private IntWritable res = new IntWritable(1);
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;int delivery=0;
      for (Text val : values) {

	String value=val.toString();
	String array[]=value.split(",");
	sum+=Integer.parseInt(array[0]);
	delivery+=Integer.parseInt(array[1]);
	
	       
}
	String val="1";
      result.set(sum);
      context.write(key, new Text(""+sum+","+delivery));
    }
  }
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount_1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
