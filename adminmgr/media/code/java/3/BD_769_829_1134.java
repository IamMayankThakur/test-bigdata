import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


public class BD_769_829_1134 {
public static class Task4Mapper
   extends Mapper<LongWritable, Text , Text, IntWritable>{
String venue;
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
   String[] values= value.toString().split(",");
   IntWritable runs=new IntWritable(0);
   if(values[0].equals("info") && values[1].equals("venue")) {
    int j=2;
    venue=values[j];
    if(venue.charAt(0)==34) {
    venue=venue+","+values[3];
    }
   
   }
if(values[0].equals("ball") && values[8].charAt(0)==48) {
       String keys = venue+","+values[4];
       runs.set(Integer.parseInt(values[7]));
context.write(new Text(keys),runs);
       }
   }
}


public static class Task4Reducer
   extends Reducer<Text,IntWritable, Text, NullWritable> {
public Map<String,Integer> map=new LinkedHashMap<String,Integer>();
String previousVenue="x";
int previousSR=0;
public void reduce(Text key, Iterable<IntWritable> values,
                  Context context) throws IOException, InterruptedException {

    int ballCount = 0;
   int runCount = 0;
   int strikeRate=0;
   for (IntWritable value : values) {
            runCount += value.get();
            ballCount++;
}
if(ballCount>=10)
{
strikeRate = (runCount*100)/ballCount;
String alldata=key.toString()+","+Integer.toString(ballCount);
//System.out.print("hi");
//System.out.print(alldata);
map.put(alldata,new Integer(strikeRate));
}
}

public void cleanup(Context context) throws IOException,InterruptedException
{

List<Map.Entry<String,Integer>> list=new ArrayList<>(map.entrySet());
// Comparator<Map.Entry<String,Integer>> Comp
//      = Comparator.reverseOrder(Map.Entry.comparingByValue())
//        .thenComparing(Employee::getName);
Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
           @Override
           public int compare(Map.Entry<String, Integer> d1, Map.Entry<String, Integer> d2){
               //sorting on venue
            Integer RunsScored_a,RunsScored_b;
            String venue_a=d1.getKey().split(",")[0];
               String venue_b=d2.getKey().split(",")[0];
               int ans=venue_a.compareTo(venue_b);
               if(ans!=0)
                return ans;
           
            //sorting on strikerate
            int strikerate = d2.getValue().compareTo(d1.getValue());
               if (strikerate != 0) {
                   return strikerate;
               }
              String temporary1=d1.getKey().split(",")[0];
              if(temporary1.charAt(0)==34) {
              RunsScored_a=Integer.parseInt(d1.getKey().split(",")[3]);
              }
              else {
              RunsScored_a=Integer.parseInt(d1.getKey().split(",")[2]);
              }
              String temporary2=d1.getKey().split(",")[0];
              if(temporary2.charAt(0)==34) {

               RunsScored_b=Integer.parseInt(d2.getKey().split(",")[3]);
              }
              else {
              RunsScored_b=Integer.parseInt(d2.getKey().split(",")[2]);
              }            
               return  RunsScored_b.compareTo(RunsScored_a);
//                if (rscored!= 0) {
//                    return rscored;
//                }
              //return venue_a.compareTo(venue_b);
           }});
   String previousVenue="";
for(Map.Entry<String, Integer> entry:list) {
String temp=entry.getKey();
String batsman;
String currentVenue=temp.split(",")[0];
String tempz=currentVenue;
if(!(previousVenue.equals(currentVenue))) {
previousVenue=currentVenue;
if(currentVenue.charAt(0)==34) {
    currentVenue=currentVenue+","+temp.split(",")[1];
    batsman=temp.split(",")[2];
    }
else {
batsman=temp.split(",")[1];
}
String finalz=currentVenue+","+batsman;
currentVenue=tempz;
context.write(new Text(finalz),null);
}
previousVenue=tempz;
currentVenue=tempz;

}
}
}


public static void main(String[] args) throws Exception {

Configuration conf = new Configuration();

Job job = Job.getInstance(conf, "ipl");
job.setJarByClass(BD_769_829_1134.class);
job.setMapperClass(Task4Mapper.class);
job.setReducerClass(Task4Reducer.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
job.setOutputKeyClass(IntWritable.class);
job.setOutputValueClass(NullWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0:1);
}
}