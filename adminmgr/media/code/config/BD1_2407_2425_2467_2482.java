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
 
public class BatsmanVul {
 
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
 
    private final static IntWritable first = new IntWritable(1);
    private Text dataout = new Text();
 
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
  String newline=value.toString();
  String dataoutarr[]=newline.split(",");
  if(dataoutarr[0].equals("ball"))
  {
    
    dataout.set(dataoutarr[4]+","+dataoutarr[6]);
    //context.write(dataout,first);
    if(dataoutarr.length>9 && (dataoutarr[9].equals("bowled") || dataoutarr[9].equals("lbw") || dataoutarr[9].equals("caught")))
  {
    dataout.set(dataoutarr[4]+","+dataoutarr[6]);
    context.write(dataout,new Text("1"+","+"1"));
  }
    else
    {
    dataout.set(dataoutarr[4]+","+dataoutarr[6]);
    context.write(dataout,new Text("1"+","+"0"));
  }
  }
        
    }
  }
 
  public static class IntvaluttlReducer
       extends Reducer<Text,Text,Text,Text> {
    private IntWritable outputdata = new IntWritable();
 
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int valuttl = 0;int wickets=0;
      for (Text val : values) {
  String value=val.toString();
  String dataoutarr[]=value.split(",");
  valuttl+=Integer.parseInt(dataoutarr[0]);
  wickets+=Integer.parseInt(dataoutarr[1]);        
}
  String val="1";
      outputdata.set(valuttl);
      context.write(key, new Text(""+valuttl+","+wickets));
    }
  }
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "dataout count");
    job.setJarByClass(dataoutCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntvaluttlReducer.class);
    job.setReducerClass(IntvaluttlReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
