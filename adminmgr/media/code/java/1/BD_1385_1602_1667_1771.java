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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class bavbo 
{
	class Player
{
    String batsman;
    String bowler;
     
    int wickets;
    int balls;
     
    public Player(String batsman,String bowler, int wickets, int balls) 
    {
        this.batsman = batsman;
        this.bowler = bowler;
        this.wickets = wickets;
        this.balls = balls ;
    }
}
	//wicketsCompare Class to compare the wickets
 
class wicketsCompare implements Comparator<Player>
{
    
    public int compare(Player s1, Player s2)
    {
        int diff_wickets=s2.wickets - s1.wickets;
        if (diff_wickets !=0) 
            return diff_wickets;
        else
        {
            int diff_balls=s1.balls - s2.balls;
            if (diff_balls==0) 
                return (s1.batsman).compareTo(s2.batsman);
            else
                return diff_balls;
                
            
        }

                
        
    }
}

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>
       {


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
      StringTokenizer itr = new StringTokenizer(value.toString(),","); //Splits the input based on spaces and newlines
	  int c=0;
	  String batsman="",bowler="";
	  int runs=-2;
      while (itr.hasMoreTokens()) 
      {
		//Extracting the fields from the dataset
	    String token=itr.nextToken();
	    if(c==0&&(token.equals("version")||token.equals("info")))
			return;
		if(c==4)
			batsman=token.trim();
		else if(c==6)
			bowler=token.trim();
		else if(c==7)
			runs=Integer.parseInt(token.trim());
		else if(c==9&&(token.trim().charAt(0)>='a'&&token.trim().charAt(0)<='z'&&token.trim().charAt(0)!='r'))
			runs=-1;
		c++;
      }
      context.write(new Text(batsman+","+bowler), new IntWritable(runs));
    }
 		}

  public class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int balls=0, wickets = 0;
      for (IntWritable val : values) {
		int temp=val.get();
		/*if(temp==0)//Counting the number of 0s, 1s, 2s, 3s, 4s, 6s and Wickets by a batsman against a particular bowler
			dot++;
		else if(temp==1)
			one++;
		else if(temp==2)
			two++;
		else if(temp==3)
			three++;
		else if(temp==4)
			four++;
		else if(temp==6)
			six++;
		else if(temp==-1)
			wickets++; */
		if(temp==-1)
			wickets++;
		balls++;
      }
	  //context.write(key, new Text(Double.toString(dot/balls)+","+Double.toString(one/balls)+","+Double.toString(two/balls)+","+Double.toString(three/balls)+","+Double.toString(four/balls)+","+Double.toString(six/balls)+","+Double.toString(1-(wickets/balls))));//Output - Player to Player Probabilities
	   ArrayList<Player> records = new ArrayList<Player>();
	  if(balls>5 && wickets > 0)
	  {
	  	String[] names=key.toString().split(",");
	   records.add(new Player(names[0],names[1],wickets,balls));
	   Collections.sort(records, new wicketsCompare());
	   //context.write(key, new Text(Integer.toString(wickets)+","+ Integer.toString(balls)));//Output
	   }
	   //for (Player res : records) 
        //{
        	//Text k=new Text(res.batsman+","+res.bowler);
            //context.write(res.batsman);
            //context.write(","+res.bowler);
            //context.write(","+res.wickets);
            //context.write(","+res.balls);
            context.write(new Text(records.get(0)+","+records.get(1)),new Text(","+records.get(2)+","+records.get(3)));
            //context.newLine();
        //}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "IPL Data Extraction");
    job.setJarByClass(bavbo.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
