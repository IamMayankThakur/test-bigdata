import java.io.IOException;
import java.io.*;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BD_0119_0142_0150_1421{

 public static int stringCompare(String str1, String str2)
        {
          //from geeksforgeeks
          int l1 = str1.length();
          int l2 = str2.length();
          int lmin = Math.min(l1, l2);

          for (int i = 0; i < lmin; i++) {
              int str1_ch = (int)str1.charAt(i);
              int str2_ch = (int)str2.charAt(i);

              if (str1_ch != str2_ch) {
                  return str1_ch - str2_ch;
              }
          }

          if (l1 != l2) {
              return l1 - l2;
          }

          else {
              return 0;
          }
      }

    public static int count = 0;

    public static ArrayList<String> resultList = new ArrayList<String>();

    public static class BMapper
       extends Mapper<Object, Text, Text, IntArrayWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] values = line.split(",");

        if(values[0].equals("ball")){
                int[] balls = new int[2];
                if(values[9].length() != 2)
                {
                    if(values[9].equalsIgnoreCase("run out") || values[9].equalsIgnoreCase("retired hurt"))
                    {
                        // System.out.println("Not correct");
                        balls[0] = 1;
                        balls[1] = 0;
                        IntArrayWritable ball_data = new IntArrayWritable(balls);
                        context.write(new Text(values[4] + "," + values[6]), ball_data);
                    }
                    else
                    {
                        balls[0] = 1;
                        balls[1] = 1;
                        IntArrayWritable ball_data = new IntArrayWritable(balls);
                        context.write(new Text(values[4] + "," + values[6]), ball_data);
                    }
                }
                else
                {
                    balls[0] = 1;
                    balls[1] = 0;
                    IntArrayWritable ball_data = new IntArrayWritable(balls);
                    context.write(new Text(values[4] + "," + values[6]), ball_data);
                }
            }   
        }
    }
    
public static class BReducer
  extends Reducer<Text, IntArrayWritable, Text, Text> {

    
    private ArrayList<String> keysofar = new ArrayList<String>();
    private Text result = new Text();
    private int count = 0;
    
    private int totalBalls;
    public void reduce(Text key, Iterable<IntArrayWritable> values,
                    org.apache.hadoop.mapreduce.Reducer<Text, IntArrayWritable, Text, Text>.Context context
                    ) throws IOException, InterruptedException {
        int wickets = 0;
        int runs = 0;
        totalBalls = 0;

        for (IntArrayWritable val : values) {
            Writable[] vals = val.get();
            wickets += Integer.valueOf(vals[1].toString());
            totalBalls += 1;

        }

        if(totalBalls > 5){
            result.set(new Text(key + "," + Integer.toString(wickets) + "," + Integer.toString(totalBalls)));
            
     	    keysofar.add(key + ","+ Integer.toString(wickets) + "," + Integer.toString(totalBalls));
            count++;
            // System.out.println(count);
        }
        // System.out.println(count);
            if(count == 9358){
                count++;	
                Collections.sort(keysofar, new Comparator<String>(){
                    
                    @Override
                    public int compare(String a, String b){
                    int wicksA = Integer.parseInt(a.split(",")[2]);
                    int wicksB = Integer.parseInt(b.split(",")[2]);
        
                    int ballsA = Integer.parseInt(a.split(",")[3]);
                    int ballsB = Integer.parseInt(b.split(",")[3]);
                    
                    String bowlerA = a.split(",")[0];
                    String bowlerB = b.split(",")[0];
                    
                    if(wicksA > wicksB){
                        return -1;			
                    }
                    else if(wicksA == wicksB){
                        if(ballsA < ballsB) return -1;
                        else if(ballsA == ballsB){
                                int x = stringCompare(bowlerA, bowlerB);
                                if(x == 0) return 0;
                                else if(x > 0) return 1;
                                else return -1;					
                            }
                        else return 1;
                    }
                    return 1;
        }
                     
                });
                for(String v: keysofar){
                    context.write(new Text(v), new Text(""));		
                }
                
            }
            


        // System.out.println(resultList);
            }

        }


    static class IntArrayWritable extends ArrayWritable {

        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(int[] integers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[integers.length];
            for (int i = 0; i < ints.length; i++) {
                ints[i] = new IntWritable(integers[i]);
            }
            set(ints);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "bd task2");
        job1.setJarByClass(BD_0119_0142_0150_1421.class);
        job1.setMapperClass(BMapper.class);
    //    job1.setCombinerClass(BReducer.class);
        job1.setReducerClass(BReducer.class);
         job1.setOutputKeyClass(Text.class);
    
        job1.setMapOutputValueClass(IntArrayWritable.class);


        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (!job1.waitForCompletion(true)) {
            // System.exit(1);

        }
    }
    
}
