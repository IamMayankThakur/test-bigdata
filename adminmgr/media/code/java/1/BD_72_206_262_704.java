import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.io.*;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.DataOutputStream;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
public class BD_72_206_262_704{
	public static class BatBowlWritable implements WritableComparable<BatBowlWritable>{
		private Text batsman;
		private Text bowler;

		public BatBowlWritable(){
			this.batsman = new Text();
			this.bowler = new Text();
		}

		public BatBowlWritable(Text batsman,Text bowler){
			this.batsman = batsman;
			this.bowler = bowler;
		}

		public void set(Text batsman, Text bowler){
			this.batsman = batsman;
			this.bowler = bowler;
		}
		public Text getBatsman() {
			return batsman;
		    }

		    public Text getBowler() {
			return bowler;
		    }

		    public void setBatsman(Text batsman) {
			       this.batsman = batsman;
		    }

		    public void setBowler(Text bowler) {
			this.bowler = bowler;
		    }

		    public void readFields(DataInput in) throws IOException {
			batsman.readFields(in);
			bowler.readFields(in);
		    }

		    public void write(DataOutput out) throws IOException {
			batsman.write(out);
			bowler.write(out);

		    }

		@Override
		    public String toString() {
			return this.batsman.toString() + "," + this.bowler.toString();
		}
		@Override
		public int compareTo(BatBowlWritable b){
			int batcheck = batsman.compareTo(b.getBatsman());
			
			if (batcheck != 0){
				return batcheck;
			}else{
				int bowlcheck = bowler.compareTo(b.getBowler());
				if (bowlcheck!=0){
					return bowlcheck;
				}else{
					return 0;
				}
			}
		}

	}
	public static class FullKeyComparator extends WritableComparator {
 
		public FullKeyComparator() {
		    super(DeliveryWritable.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable wc1, WritableComparable wc2) {
		    
		    DeliveryWritable key1 = (DeliveryWritable) wc1;
		    DeliveryWritable key2 = (DeliveryWritable) wc2;
		    
		//     int batCmp = key1.getBatsman().compareTo(key2.getBatsman());
		//     if (batCmp != 0) {
		// 	return batCmp;
		//     } else {
		// 	int bowlCmp = key1.getBowler().compareTo(key2.getBowler());
		// 	if (bowlCmp != 0) {
		// 	    return bowlCmp;
		// 	} else {
		// 	    return 0;
		// 	}
		//     }
			//return (key1.getBatBowl().compareTo(key2.getBatBowl()))*-1;
			if (key1.getBatBowl().compareTo(key2.getBatBowl())==0){
				return 0;
			}else if (key1.getWickets().compareTo(key2.getWickets())==0){
				//same wickets, need to check deliveries
				if (key1.getDeliveries().compareTo(key2.getDeliveries())==0){
					//another tie, go for name and call it a day
					return (key1.getBatBowl().compareTo(key2.getBatBowl()));
				}
				return key1.getDeliveries().compareTo(key2.getDeliveries());
			}else{
				return (key1.getWickets().compareTo(key2.getWickets()))*-1;
			}
			
		}
	}
	public static class WickBallWritable implements Writable,WritableComparable<WickBallWritable>{
		private IntWritable wickets;
		private IntWritable deliveries;

		public WickBallWritable(){
			this.wickets = new IntWritable();
			this.deliveries = new IntWritable();
		}
		public WickBallWritable(WickBallWritable m){
			this.wickets = new IntWritable(m.getWickets().get());
			this.deliveries = new IntWritable(m.getDeliveries().get());
		}
		public WickBallWritable(IntWritable wickets, IntWritable deliveries){
			this.deliveries = deliveries;
			this.wickets = wickets;
		}

		public void set(IntWritable wickets, IntWritable deliveries){
			this.deliveries = deliveries;
			this.wickets = wickets;
		}

		   public IntWritable getWickets() {
			return this.wickets;
		    }

		    public IntWritable getDeliveries() {
			return this.deliveries;
		    }

		    public void setWickets(IntWritable wickets) {
			this.wickets = wickets;
		    }

		    public void setDeliveries(IntWritable deliveries) {
			this.deliveries = deliveries;
		    }

		    public void readFields(DataInput in) throws IOException {
			this.wickets.readFields(in);
			this.deliveries.readFields(in);
		    }

		    public void write(DataOutput out) throws IOException {
			this.wickets.write(out);
			this.deliveries.write(out);
		    }
		@Override
		public int compareTo(WickBallWritable w){
			return (this.wickets.compareTo(w.getWickets()));
		}
		@Override
		    public String toString() {
			return this.wickets.toString() + "," + this.deliveries.toString();
		}
	}
	public static class DeliveryWritable implements Writable,WritableComparable<DeliveryWritable>{
		private Text batsman;
		private Text bowler;
		private IntWritable wickets;
		private IntWritable deliveries;

		public DeliveryWritable(){
			this.batsman = new Text();
			this.bowler = new Text();
			this.wickets = new IntWritable();
			this.deliveries = new IntWritable();
		}

		public DeliveryWritable(Text batsman,Text bowler,IntWritable wickets, IntWritable deliveries){
			this.batsman = batsman;
			this.bowler = bowler;
			this.deliveries = deliveries;
			this.wickets = wickets;
		}

		public void set(Text batsman, Text bowler,IntWritable wickets, IntWritable deliveries){
			this.batsman = batsman;
			this.bowler = bowler;
			this.deliveries = deliveries;
			this.wickets = wickets;
		}
		public Text getBatsman() {
			return this.batsman;
		    }

		    public String getBatBowl() {
			return this.batsman.toString()+"-"+this.bowler.toString();
		    }

		    public Text getBowler() {
			return this.bowler;
		    }

		   public IntWritable getWickets() {
			return this.wickets;
		    }

		    public IntWritable getDeliveries() {
			return this.deliveries;
		    }

		    public void setBatsman(Text batsman) {
			       this.batsman = batsman;
		    }

		    public void setBowler(Text bowler) {
			this.bowler = bowler;
		    }

		    public void setWickets(IntWritable wickets) {
			this.wickets = wickets;
		    }

		    public void setDeliveries(IntWritable deliveries) {
			this.deliveries = deliveries;
		    }

		    public void readFields(DataInput in) throws IOException {
			batsman.readFields(in);
			bowler.readFields(in);
			wickets.readFields(in);
			deliveries.readFields(in);
		    }

		    public void write(DataOutput out) throws IOException {
			batsman.write(out);
			bowler.write(out);
			wickets.write(out);
			deliveries.write(out);
		    }

		@Override
		    public String toString() {
			return batsman.toString() + "," + bowler.toString() + "," + wickets.toString() + "," + deliveries.toString();
		}
		@Override
		public int compareTo(DeliveryWritable b){
			// if ((batsman.compareTo(b.getBatsman())==0) &&  (bowler.compareTo(b.getBowler())==0)){
			// 	return 0;
			// }
			// if (this.getBatBowl().compareTo(b.getBatBowl())==0){
			// 	return 0;
			// }
			// if (wickets.compareTo(b.getWickets())==0){
			// 	//same wickets, need to check deliveries
			// 	if (deliveries.compareTo(b.getDeliveries())==0){
			// 		//another tie, go for name and call it a day
			// 		return (this.getBatBowl().compareTo(b.getBatBowl()));
			// 	}
			// 	return deliveries.compareTo(b.getDeliveries());
			// }
			// return (wickets.compareTo(b.getWickets()))*-1;
			return this.getBatBowl().compareTo(b.getBatBowl());
		}

	}

	public static class CustomOutputFormat extends FileOutputFormat<DeliveryWritable, WickBallWritable> {
		public CustomOutputFormat(){};
		@Override
		public org.apache.hadoop.mapreduce.RecordWriter<DeliveryWritable, WickBallWritable> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		   //get the current path
		   Path path = FileOutputFormat.getOutputPath(arg0);
		   //create the full path with the output directory plus our filename
		   Path file = getDefaultWorkFile(arg0,"");
	       //create the file in the file system
	       FileSystem fs = path.getFileSystem(arg0.getConfiguration());
	       FSDataOutputStream fileOut = fs.create(file, arg0);
	  
	       //create our record writer with the new file
	       return new CustomRecordWriter(fileOut);
	    }
	  }
	  
	public static class CustomRecordWriter extends RecordWriter<DeliveryWritable, WickBallWritable> {
	      private DataOutputStream out;
	  
	      public CustomRecordWriter(DataOutputStream stream) {
		  out = stream;
		  try {
		      //out.writeBytes("results:\r\n");
		  }
		  catch (Exception ex) {
		  }  
	      }
	  
	      @Override
	      public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		  //close our file
		  out.close();
	      }
	  
	      @Override
	      public void write(DeliveryWritable arg0, WickBallWritable arg1) throws IOException, InterruptedException {
		  //write out our key
		  out.writeBytes(arg0.getBatsman().toString() + ","+arg0.getBowler().toString()+","+arg1.getWickets().toString()+","+arg1.getDeliveries().toString());
		  //loop through all values associated with our key and write them with commas between
		  out.writeBytes("\r\n");  
	      }
	}


	
	
	
	private static class TokenizerMapper extends Mapper<Object, Text, DeliveryWritable, WickBallWritable>{

		private Text batsman = new Text();
		private Text bowler = new Text();
		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);
		private BatBowlWritable batbowl = new BatBowlWritable();
		private WickBallWritable wickball = new WickBallWritable();
		private DeliveryWritable delw = new DeliveryWritable();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			//StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			//while (itr.hasMoreTokens()){
				String[] data = value.toString().split(",",0);
				int len = data.length;
				if (data[0].equals("ball")){
					batsman.set(data[4]);bowler.set(data[6]);
					//batbowl.set(batsman,bowler);
					
					//if (data[8].equals("0")){
						if (len > 9) {
							if(data[9].equals("obstructing the field")||data[9].equals("stumped")||data[9].equals("hit wicket") || data[9].equals("caught") || data[9].equals("bowled") || data[9].equals("lbw") || data[9].equals("caught and bowled")){
								wickball.set(one,one);
								delw.set(batsman,bowler,one,one);
							}else{
								wickball.set(zero,one);
								delw.set(batsman,bowler,zero,one);
							}
						}else{
							wickball.set(zero,one);
							delw.set(batsman,bowler,zero,one);
						}
						context.write(delw,wickball);
					//}
				}
			}

	}//}
	// private static class SortingMapper extends Mapper<Object, Text, DeliveryWritable, Text>{
	// 	private Text batsman = new Text();
	// 	private Text bowler = new Text();
	// 	private IntWritable wickets = new IntWritable();
	// 	private IntWritable delivery = new IntWritable();
	// 	private DeliveryWritable del = new DeliveryWritable();
	// 	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	// 		StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
	// 		while (itr.hasMoreTokens()){
	// 			String[] data = itr.nextToken().split(",",0);
	// 			batsman.set(data[0]);
	// 			bowler.set(data[1]);
	// 			wickets.set(Integer.parseInt(data[2]));
	// 			delivery.set(Integer.parseInt(data[3].trim()));
	// 			del.set(batsman,bowler,wickets,delivery);
	// 			context.write(del,new Text(""));
	// 		}
	// 	}}
	public static class KeyComparator implements Comparator<DeliveryWritable> {
 
		public int compare(DeliveryWritable key1, DeliveryWritable key2) {
		    
			if (key1.getBatBowl().compareTo(key2.getBatBowl())==0){
				return 0;
			}else if (key1.getWickets().compareTo(key2.getWickets())==0){
				//same wickets, need to check deliveries
				if (key1.getDeliveries().compareTo(key2.getDeliveries())==0){
					//another tie, go for name and call it a day
					return (key1.getBatsman().compareTo(key2.getBatsman()));
				}
				return key1.getDeliveries().compareTo(key2.getDeliveries());
			}else{
				return (key1.getWickets().compareTo(key2.getWickets()))*-1;
			}
			
		}
	}
	public static class DeliveryReducer extends Reducer<DeliveryWritable,WickBallWritable,DeliveryWritable,WickBallWritable> {
    		private IntWritable wicket_result = new IntWritable();
		private IntWritable delivery_result = new IntWritable();
		
		private DeliveryWritable d = new DeliveryWritable();
		private WickBallWritable ww = new WickBallWritable();
		private Text empty = new Text("");
		private ArrayList<DeliveryWritable> ar = new ArrayList<DeliveryWritable>();
		//private Text result = new Text();
		//private MultipleOutputs output;

		// @Override
		// public void setup(Context context)
		// {
		// 	output = new MultipleOutputs(context);
		// }

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{	
			Collections.sort(ar, new KeyComparator()); 
			for (int i=0; i<ar.size(); i++) {
				context.write(ar.get(i),new WickBallWritable(ar.get(i).getWickets(),ar.get(i).getDeliveries()));
			}
            			
		}
    		public void reduce(DeliveryWritable b, Iterable<WickBallWritable> w, Context context) throws IOException, InterruptedException {
			int sum_wickets = 0; int sum_balls = 0;
			      
      			for (WickBallWritable val : w) {
        			sum_wickets += val.getWickets().get();
				sum_balls += val.getDeliveries().get();
			}
			//d = new DeliveryWritable();
			DeliveryWritable d = new DeliveryWritable();
			WickBallWritable ww = new WickBallWritable();
			wicket_result.set(sum_wickets);
			delivery_result.set(sum_balls);
			if (sum_balls > 5){
				//d.set(b.getBatsman(),b.getBowler(),wicket_result,delivery_result);
				//output.write("inter", d,new Text(""));
				ww.set(wicket_result,delivery_result);
				d.set(new Text(b.getBatsman().toString()),new Text(b.getBowler().toString()),new IntWritable(wicket_result.get()),new IntWritable(delivery_result.get()));
				//context.write(d,ww);
				ar.add(d);
			}
    		}
	}
// 	public static class FinalReducer extends Reducer<DeliveryWritable,WickBallWritable,DeliveryWritable,WickBallWritable> {
// 		private IntWritable wicket_result = new IntWritable();
// 	    private IntWritable delivery_result = new IntWritable();
	    
// 	    private DeliveryWritable d = new DeliveryWritable();
// 	    private WickBallWritable ww = new WickBallWritable();
// 	    private Text empty = new Text("");
	    
// 	    @Override
// 	    public void cleanup(Context context) throws IOException, InterruptedException
// 	    {
		    

// 	    }
// 		public void reduce(DeliveryWritable b, Iterable<WickBallWritable> w, Context context) throws IOException, InterruptedException {
		    
// 		}
//     }
	public static class CustomPartitioner extends Partitioner<DeliveryWritable,WickBallWritable>{
		@Override
		public int getPartition(DeliveryWritable dw,WickBallWritable w,int numberOfPartitions) {

         		return Math.abs(dw.getBatsman().hashCode() % numberOfPartitions);
    		}
	}

	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf, "BD_72_206_262_704");
    		job.setJarByClass(BD_72_206_262_704.class);
    		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(DeliveryReducer.class);
		//job.setSortComparatorClass(FullKeyComparator.class);
		job.setReducerClass(DeliveryReducer.class);
		//job.setPartitionerClass(CustomPartitioner.class);
		//job.setGroupingComparatorClass(FullKeyComparator.class);
		
    		job.setOutputKeyClass(DeliveryWritable.class);
		job.setOutputValueClass(WickBallWritable.class);
		job.setOutputFormatClass(CustomOutputFormat.class);
		//MultipleOutputs.addNamedOutput(job, "inter", TextOutputFormat.class,DeliveryWritable.class,Text.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
