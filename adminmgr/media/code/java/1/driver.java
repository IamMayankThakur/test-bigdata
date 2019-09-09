import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 

public class driver { 

    public static void main(String[] args) throws Exception 
    { 
       
        Configuration conf = new Configuration(); 
        conf.set("mapred.textoutputformat.separator",",");
        String[] otherArgs = new GenericOptionsParser(conf, 
                                args).getRemainingArgs(); 

        // if less than two paths 
        // provided will show error 
        if (otherArgs.length < 2) 
        { 
            System.err.println("Error: please provide two paths"); 
            System.exit(2); 
        } 

        Job job = Job.getInstance(conf, "please..."); 
        job.setJarByClass(driver.class); 

        job.setMapperClass(mapper_test.class); 
        job.setReducerClass(reducer_test.class); 

        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(IntWritable.class); 

        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntArrayWritable.class); 

        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 
