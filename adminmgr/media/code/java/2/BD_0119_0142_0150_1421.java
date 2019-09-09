import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BD_0119_0142_0150_1421{
public static class SortByRuns implements Comparator<String>{
		public int compare(String a, String b){
			int runsA = Integer.parseInt(a.split(",")[2]);
			int runsB = Integer.parseInt(b.split(",")[2]);

			int ballsA = Integer.parseInt(a.split(",")[3]);
			int ballsB = Integer.parseInt(b.split(",")[3]);
			
			String bowlerA = a.split(",")[0];
			String bowlerB = b.split(",")[0];
			
			if(runsA > runsB){
				return 1;			
			}
			else if(runsA == runsB){
				if(ballsA < ballsB) return 1;
				else if(ballsA == ballsB) return 0;
				else return -1;
			}
			return -1;
 			
			
				
		}	
	}
 public static int stringCompare(String str1, String str2)
        {
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
  public static class BMapper
       extends Mapper<Object, Text, Text, IntArrayWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException{
      String line = value.toString();
      String[] values = line.split(",");

      if(values[0].contains("ball")){
		    int[] balls = new int[2];
        balls[0] = Integer.parseInt(values[8]);
        balls[1] = Integer.parseInt(values[7]);
        IntArrayWritable ball_data = new IntArrayWritable(balls);
        context.write(new Text(values[6].trim() + "," + values[4].trim()), ball_data);
	}
    }
  }

  public static class BReducer
       extends Reducer<Text, IntArrayWritable, Text, Text> {
	private int BallsUntilNow = 0;
	private ArrayList<String> keysofar = new ArrayList<String>();
	private Text result = new Text();
private int totalBalls;
    public void reduce(Text key, Iterable<IntArrayWritable> values,
                        org.apache.hadoop.mapreduce.Reducer<Text, IntArrayWritable, Text, Text>.Context context
                       ) throws IOException, InterruptedException {
      int extraballsnum = 0;
      int runs = 0;
      totalBalls = 0;

      for (IntArrayWritable val : values) {
	BallsUntilNow += 1;
        Writable[] vals = val.get();
        extraballsnum = Integer.valueOf(vals[0].toString());
       totalBalls += 1;
	 if(extraballsnum == 0){
           
           runs += Integer.valueOf(vals[1].toString());
         }

         else{
          runs += extraballsnum + Integer.valueOf(vals[1].toString());
         }

      }
	
	if(totalBalls > 5){
      result.set(new Text("," + Integer.toString(runs) + "," + Integer.toString(totalBalls)));
     	keysofar.add(key + ","+ Integer.toString(runs) + "," + Integer.toString(totalBalls));
	}
	if(BallsUntilNow >= 163547){	
		Collections.sort(keysofar, new Comparator<String>(){
			
			@Override
			public int compare(String a, String b){
			int runsA = Integer.parseInt(a.split(",")[2]);
			int runsB = Integer.parseInt(b.split(",")[2]);

			int ballsA = Integer.parseInt(a.split(",")[3]);
			int ballsB = Integer.parseInt(b.split(",")[3]);
			
			String bowlerA = a.split(",")[0];
			String bowlerB = b.split(",")[0];
			
			if(runsA > runsB){
				return -1;			
			}
			else if(runsA == runsB){
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
    Job job = Job.getInstance(conf, "bd task3");
    job.setJarByClass(BD_0119_0142_0150_1421.class);
    job.setMapperClass(BMapper.class);
//    job.setCombinerClass(BReducer.class);
    job.setReducerClass(BReducer.class);
    job.setOutputKeyClass(Text.class);

    job.setMapOutputValueClass(IntArrayWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

//I think it's type, matchno, over, team batting, on strike batsman, non strike batsman, bowler, //runs, extras, how the batsman got out, who got out

