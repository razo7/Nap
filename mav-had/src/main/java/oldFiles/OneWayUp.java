package oldFiles;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class OneWayUp extends Configured implements Tool{
	public static class XMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		private int ANum;
		private int BNum;
		
		private long TaskTime;
		private static final Log LOG = LogFactory.getLog(XMapper.class);
		  @Override
		    protected void setup(Context context) 
		  {
		        Configuration c = context.getConfiguration();
		        ANum = Integer.parseInt(c.get("ANum"));
		        BNum = Integer.parseInt(c.get("BNum"));
		      
		        TaskTime = new Date().getTime();
		    } 
		  @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String reducerIndex;
			//System.out.println("HEY map1: " + value); //log to stdout file
			String line = value.toString();
			String[] splitInput = line.split("\\t");
			LOG.info("splitInput[0] : " +line);
			if (splitInput.length == 3)
			{
			Text person = new Text(splitInput[0]);
			Text Fname = new Text(splitInput[1]);
			Text Lname = new Text(splitInput[2]);

			Text opString = new Text("X" + person + "," + Fname +"," + Lname );
			String a1 = String.valueOf(1 + (splitInput[0].hashCode() & Integer.MAX_VALUE % ANum));
			for (int i = 1; i <= BNum; i++) 
			{
				String b1 = String.valueOf(i);
				reducerIndex = a1 + b1;
				//System.out.println("map1 : "+reducerIndex);
				LOG.info("Logging map1 X keys! : " +reducerIndex);
				context.write(new Text(reducerIndex), opString);
			}
			
			
			LOG.info("Logging map1 X value! : " +opString);
			}//if length
		}// map
		  @Override
		protected void cleanup(Context context ) throws IOException, InterruptedException 
		  {
			  TaskTime = new Date().getTime() -TaskTime;
			  
		  }//cleanup
	}// XMapper Class

	public static class YMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		private int ANum;
		private int BNum;
		private static final Log LOG = LogFactory.getLog(YMapper.class);
	  @Override
	    protected void setup(Context context) 
	  {
	        Configuration c = context.getConfiguration();
	        ANum = Integer.parseInt(c.get("ANum"));
	        BNum = Integer.parseInt(c.get("BNum"));
	    } 
	  @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();
			String[] splitInput = line.split("\\t");

			Text a = new Text(splitInput[0]);
			Text b = new Text(splitInput[1]);

			Text opString = new Text("Y" + a + "," + b);
			String a1 = String.valueOf(1 + (splitInput[0].hashCode() & Integer.MAX_VALUE % ANum));
			String b1 = String.valueOf(1 + (splitInput[1].hashCode() & Integer.MAX_VALUE % BNum));
			context.write(new Text( a1 + b1), opString);
			LOG.info("Logging map2 Y value! : " +opString);
		}// map

	}// YMapper Class

	public static class ZMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		private int ANum;
		private int BNum;

		private static final Log LOG = LogFactory.getLog(ZMapper.class);
		
		  @Override
		    protected void setup(Context context) 
		  {
		        Configuration c = context.getConfiguration();
		        ANum = Integer.parseInt(c.get("ANum"));
		        BNum = Integer.parseInt(c.get("BNum")); 
		    } 
		  @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String reducerIndex;
			String line = value.toString();
			String[] splitInput = line.split("\\t");

			Text b = new Text(splitInput[0]);
			Text c = new Text(splitInput[1]);

			Text opString = new Text("Z" + b + "," + c);
			String b1 = String.valueOf(1 + (splitInput[0].hashCode() & Integer.MAX_VALUE % BNum));
			
			for (int i = 1; i <= ANum; i++) 
			{
				String a1 = String.valueOf(i);
				reducerIndex = a1 + b1;
			//	System.out.println("map3 : "+reducerIndex);
				LOG.info("Logging map3 Z keys! : " +reducerIndex);
				context.write(new Text(reducerIndex), opString);
			}
			LOG.info("Logging map3 Z value! : " +opString);
			
		}

	}// ZMapper Class

	public static class Reduce extends 	Reducer<Text, Text, Text, Text> 
	{
		private static final Log LOG = LogFactory.getLog(Reduce.class);
		ArrayList<Text> RelX;
		ArrayList<Text> RelY;
		ArrayList<Text> RelZ;
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{

			RelX = new ArrayList<Text>();
			RelY = new ArrayList<Text>();
			RelZ = new ArrayList<Text>();

			for (Text val :values ) 
			{
				String relationValue = val.toString();
				if (relationValue.indexOf('X') >= 0) 
				{
					
					String finalVal = relationValue.substring(1, relationValue.length());
					RelX.add(new Text(finalVal));
					LOG.info("Logging reduecr  X: " +key +" " + finalVal);
				}// if
				else if (relationValue.indexOf('Y') == 0) 
				{
					String finalVal = relationValue.substring(1, relationValue.length());
					RelY.add(new Text(finalVal));
					LOG.info("Logging reduecr  Y: " +key +" " + finalVal);
				}// if
				else 
				{
					String finalVal = relationValue.substring(1, relationValue.length());
					RelZ.add(new Text(finalVal));
					LOG.info("Logging reduecr  Z: " +key +" " + finalVal);
				}// else
			}// while

			for (Text r : RelX) 
			{
				String[] Xtemp = r.toString().split(",");
				for (Text s : RelY) 
				{
					String[] Ytemp = s.toString().split(",");
					if (Xtemp[0].equalsIgnoreCase(Ytemp[0])) 
					{
						for (Text t : RelZ) 
						{
							String[] Ztemp = t.toString().split(",");
							if (Ytemp[1].equalsIgnoreCase(Ztemp[0])) 
								context.write(key, new Text(Xtemp[0]+ " " + Xtemp[1]+ " " + Xtemp[2]+ " " + Ytemp[1] + " " + Ztemp[1]));
						}// for
					}// if

				}// for
			}// for
			LOG.info("Logging reduecr : " +key);
		}// reduce
		

	}// Reduce Class

	public static class newPartitionerClass extends Partitioner<Text, Text> implements  org.apache.hadoop.conf.Configurable
	  {		
		  int [] PartitionSize;
	      int W = 0 ;//sum downlinks
		  private static final Log LOG = LogFactory.getLog(newPartitionerClass.class);
	
		  @Override
		    public void setConf (Configuration conf)
		    {
			  String bwString_RM = "";
			  String bwNodeString = conf.get("bwNodeString");
			  bwString_RM = conf.get("bw_RM");
			  if(bwString_RM == null || bwString_RM == "")
			  {
		    	try {
					FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), conf);
					Path hdfsPath = new Path("/user/hadoop2/HDFS_fileFromHeartbeat");
		        	FSDataInputStream inputStream = fs.open(hdfsPath);
			        //Classical input stream usage
			        String out = IOUtils.toString(inputStream, "UTF-8");
			        bwString_RM = out.toString();			
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			  }//if
			  else
		    	{
				  LOG.info("OR_Change-getPartition- Successful conf.get\n"+ bwString_RM + " " +bwNodeString);
		    	}
		    	 if (bwString_RM == null)
			    	 LOG.info("OR_Change-getPartition- No upload-1");
		    	 else
		    	 {
		    		 String [] NodesBw = bwNodeString.split("\\s+");
		    		 String [] ReducerNodes = bwString_RM.split("\\s+");
		 	         PartitionSize = new int [ReducerNodes.length];
		 	         for (int i=0; i< ReducerNodes.length; i++)
		 	        	{
		 	        	 if (ReducerNodes[i] == "master")
		 	        		PartitionSize[i] = Integer.parseInt(NodesBw[0]);
		 	        	 if (ReducerNodes[i] == "razoldslave1-len")
			 	        	PartitionSize[i] = Integer.parseInt(NodesBw[1]);
		 	        	 else
		 	        		PartitionSize[i] = Integer.parseInt(NodesBw[2]);
		 	        	 W += PartitionSize[i];
		 	        	}
		 	        LOG.info("OR_Change-getPartition- Yes upload\n"+ bwString_RM + " " +bwNodeString);	 	     	   
		    	 }//else
		    }//setConf
		    
		    @Override
		    public Configuration getConf()
		    {
		    	return null;
		    }
		    
		  //important for partitioning tuples with the same reducer ID to the same destination(partition)
	    @Override
	    public int getPartition(Text key, Text value, int numPartitions) 
	    {    	//read time
	    
	     int res=0;
	  	 if (W == 0)
	  		 res =(key.hashCode() & Integer.MAX_VALUE) % numPartitions; 
	  	 
	  	 else
	  	 {//when we have the new allocation
	     	 res =(key.hashCode() & Integer.MAX_VALUE) % W;
	     	 int optPartit = 0;
	     	 int partitionIndicator = PartitionSize[0];
	     	 while (res > partitionIndicator)
	     	   {
	     	      optPartit++;        
	     		  partitionIndicator += PartitionSize[optPartit];
	     	    }//while
	     	 res = optPartit;
	     	 }//else

	  	 
		  return res;
	   }//fun getPartition
	  		
	}//class newPartitionerClass
	 
	@Override
    public int run (String[] args) throws Exception
    {// input_1 input_2 input_3 output inputsplitSize keySplitVector numReducers downlinkVec JobName
				Configuration conf = getConf();
				// # of mappers = size_input / split size [Bytes], split size=  max(mapreduce.input.fileinputformat.split.minsize, min(mapreduce.input.fileinputformat.split.maxsize, dfs.blocksize))
				conf.set("mapreduce.input.fileinputformat.split.minsize", args[4]); 
				conf.set("mapreduce.input.fileinputformat.split.maxsize", args[4]);
			    conf.set("mapreduce.map.log.level", "DEBUG");
			    //conf.set("mapreduce.task.profile", "true");
			    //conf.set("mapreduce.task.profile.reduces", "0-5");
			    //conf.set("mapreduce.task.timeout", "1800000"); //30 minutes wait for before killing the task
			    conf.set("bwNodeString", args[7]); // pass the downlink vector of partitions
			    String [] splitInput = args[5].split("\\s+");
				conf.set("ANum", splitInput[0]); // pass the table size
				conf.set("BNum", splitInput[1]); // pass the table size
				//conf.set("CNum", "2"); // pass the table size
				System.setProperty("hadoop.home/dir", "/");
			/*	
				Job job = Job.getInstance(conf, args[8]);
				job.setJarByClass(getClass());
				job.setReducerClass(Reduce.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				//job.setInputFormatClass(TextInputFormat.class); // needed?
				//job.setOutputFormatClass(TextOutputFormat.class); // needed?
				job.setNumReduceTasks(Integer.valueOf(args[6]));

				MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, XMapper.class);
				MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, YMapper.class);
				MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, ZMapper.class);
				FileOutputFormat.setOutputPath(job, new Path(args[3]));
				System.out.println("Downlinks list: " + args[7]);

				long start1 = new Date().getTime();
			    if (!job.waitForCompletion(true))
			    	System.exit(1);  
			    long end1 = new Date().getTime();
			     
			    return job.waitForCompletion(true)? 0 : 1;
				*/
				
				int rounds = Integer.parseInt(args[9]);
				long [] elaspeJobTimeArr = new long [rounds]; 
				double totalTime = 0;
				DecimalFormat df = new DecimalFormat();
				df.setMaximumFractionDigits(3);
				df.setMinimumFractionDigits(3);
				for (int i=0; i< rounds; i++)
				{
					elaspeJobTimeArr[i] = myRunJob(conf, args, String.valueOf(i));	
					//System.out.println("Job1 took "+ elaspeJobTimeArr[i] + " milliseconds");	
					System.out.println("Job "+ i +" took "+ ((elaspeJobTimeArr[i] /1000) /60) + " minutes and " +((elaspeJobTimeArr[i] /1000)%60) + " seconds");	
					totalTime += elaspeJobTimeArr[i];
				}
				for (int i=0; i< rounds; i++)
					 //System.out.println("Job1 took "+ elaspeJobTimeArr[i] + " milliseconds");
					System.out.println("Job "+ i +" took "+ ((elaspeJobTimeArr[i] /1000) /60) + " minutes and " +((elaspeJobTimeArr[i] /1000)%60) + " seconds");	
				 
				System.out.println("Average Job took "+ (long)((((totalTime /rounds)/1000) /60)) + " minutes and " + df.format((((totalTime /rounds)/1000)%60)) + " seconds");		
				return(0);	  
	
    }
	
	public static long myRunJob (Configuration conf, String [] args, String index) throws ClassNotFoundException, IOException, InterruptedException
	{
		Job job = Job.getInstance(conf, args[8]);
		job.setJarByClass(OneWayUp.class);
		job.setPartitionerClass(newPartitionerClass.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class); // needed?
		job.setOutputFormatClass(TextOutputFormat.class); // needed?
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(Integer.valueOf(args[6]));

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, XMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, YMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, ZMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(new Path(args[3]),index));
		System.out.println("Downlinks list: " + args[7]);

		long start1 = new Date().getTime();
	    if (!job.waitForCompletion(true))
	    	System.exit(1);  
	    return (new Date().getTime() -start1);
	}
	public static void main(String[] args) throws Exception 
	{
		int exitcode = ToolRunner.run(new OneWayUp(), args);
		System.exit(exitcode);
	}// main
	

}// OneWayUp class
