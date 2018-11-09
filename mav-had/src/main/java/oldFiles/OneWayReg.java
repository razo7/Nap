package oldFiles;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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


public class OneWayReg {
	public static String myIp ()
	    {
	    	String ip,ip2 = "";
	        Enumeration<NetworkInterface> interfaces;
			try 
			{
				interfaces = NetworkInterface.getNetworkInterfaces();
	            while (interfaces.hasMoreElements()) 
	            {
	                NetworkInterface iface = interfaces.nextElement();// filters out 127.0.0.1 and inactive interfaces
	                if (iface.isLoopback() || !iface.isUp())
	                    continue;

	                Enumeration<InetAddress> addresses = iface.getInetAddresses();
	                while(addresses.hasMoreElements()) 
	                {
	                    InetAddress addr = addresses.nextElement();
	                    ip = addr.getHostAddress();//System.out.println(iface.getDisplayName() + " " + ip + ""key);
	                    ip2 = ip.substring(11);
	                }//while
	            }//while
			}//try
			catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    return ip2;
	    }//myIp

	public static class XMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		private int ANum;
		private int BNum;
		private int CNum;
		private long TaskTime;
		private static final Log LOG = LogFactory.getLog(XMapper.class);
		  @Override
		    protected void setup(Context context) 
		  {
		        Configuration c = context.getConfiguration();
		        ANum = Integer.parseInt(c.get("ANum"));
		        BNum = Integer.parseInt(c.get("BNum"));
		        CNum = Integer.parseInt(c.get("CNum"));
		        TaskTime = new Date().getTime();
		    } 
		  @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String reducerIndex;
			//System.out.println("HEY map1: " + value); //log to stdout file
			String line = value.toString();
			String[] splitInput = line.split("\\t");

			Text a = new Text(splitInput[0]);
			Text b = new Text(splitInput[1]);

			Text opString = new Text("X" + a + "," + b);
			String a1 = String.valueOf(1 + (splitInput[0].hashCode() & Integer.MAX_VALUE % ANum));
			String b1 = String.valueOf(1 + (splitInput[1].hashCode() & Integer.MAX_VALUE % BNum));
			for (int i = 1; i <= CNum; i++) 
			{
				String c1 = String.valueOf(i);
				reducerIndex = a1 + b1 + c1;
				//System.out.println("map1 : "+reducerIndex);
				LOG.info("Logging map1 X keys! : " +reducerIndex);
				context.write(new Text(reducerIndex), opString);
			}
			
			LOG.info("Logging map1 X value! : " +opString);
			LOG.debug("Logging yey2222! : " +opString);

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
		private int CNum;
		private long TaskTime;
		private static final Log LOG = LogFactory.getLog(YMapper.class);
	  @Override
	    protected void setup(Context context) 
	  {
	        Configuration c = context.getConfiguration();
	        ANum = Integer.parseInt(c.get("ANum"));
	        BNum = Integer.parseInt(c.get("BNum"));
	        CNum = Integer.parseInt(c.get("CNum"));
	        TaskTime = new Date().getTime();
	    } 
	  @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String reducerIndex;
			String line = value.toString();
			String[] splitInput = line.split("\\t");

			Text b = new Text(splitInput[0]);
			Text c = new Text(splitInput[1]);

			Text opString = new Text("Y" + b + "," + c);
			String b1 = String.valueOf(1 + (splitInput[0].hashCode() & Integer.MAX_VALUE % BNum));
			String c1 = String.valueOf(1 + (splitInput[1].hashCode() & Integer.MAX_VALUE % CNum));
			for (int i = 1; i <= ANum; i++) {
				String a1 = String.valueOf(i);
				reducerIndex = a1 + b1 + c1;
				//System.out.println("map2 : "+reducerIndex);
				LOG.info("Logging map2 Y keys! : " +reducerIndex);
				context.write(new Text(reducerIndex), opString);
			}
			LOG.info("Logging map2 Y value! : " +opString);
		}// map

	}// YMapper Class

	public static class ZMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		private int ANum;
		private int BNum;
		private int CNum;
		private long TaskTime;
		private static final Log LOG = LogFactory.getLog(ZMapper.class);
		
		  @Override
		    protected void setup(Context context) 
		  {
		        Configuration c = context.getConfiguration();
		        ANum = Integer.parseInt(c.get("ANum"));
		        BNum = Integer.parseInt(c.get("BNum"));
		        CNum = Integer.parseInt(c.get("CNum"));
		        TaskTime = new Date().getTime();
		    } 
		  @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String reducerIndex;
			String line = value.toString();
			String[] splitInput = line.split("\\t");

			Text c = new Text(splitInput[0]);
			Text a = new Text(splitInput[1]);

			Text opString = new Text("Z" + c + "," + a);
			String c1 = String.valueOf(1 + (splitInput[0].hashCode() & Integer.MAX_VALUE % CNum));
			String a1 = String.valueOf(1 + (splitInput[1].hashCode() & Integer.MAX_VALUE % ANum));
			for (int i = 1; i <= BNum; i++) 
			{
				String b1 = String.valueOf(i);
				reducerIndex = a1 + b1 + c1;
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
					if (Xtemp[1].equalsIgnoreCase(Ytemp[0])) 
					{
						for (Text t : RelZ) 
						{
							String[] Ztemp = t.toString().split(",");
							if (Ytemp[1].equalsIgnoreCase(Ztemp[0]) && Ztemp[1].equalsIgnoreCase(Xtemp[0])) 
								context.write(key, new Text(Xtemp[0]+ " " + Ytemp[0] + " " + Ztemp[0]));
						}// for
					}// if

				}// for
			}// for
			LOG.info("Logging reduecr : " +key);
			LOG.debug("Logging reduecr y2222! : " +key);
		}// reduce
		

	}// Reduce Class

	public static class newPartitionerClass extends Partitioner<Text, Text> implements  org.apache.hadoop.conf.Configurable
	  {		
		  String bwString;
		  private static final Log LOG = LogFactory.getLog(newPartitionerClass.class);
		  @Override
		    public void setConf (Configuration conf)
		    {
		    	bwString = conf.get("bw");
		    }
		    
		    @Override
		    public Configuration getConf()
		    {
		    	return null;
		    }
		    
		  //important for partitioning tuples with the same reducer ID to the same destination(partition)
	    @Override
	    public int getPartition(Text key, Text value, int numPartitions) 
	    {    	//read time
	    	
	     int res=0,time = 1;
	  	 if (time==0)
	  	  {
	  		 res = (key.hashCode() & Integer.MAX_VALUE) % numPartitions; 
	  	  }
	  	 else
	  	 {//when we have the new allocation
	  		LOG.info("OR_Change-getPartition- Yes upload");
	  		System.out.println("OR_Change-getPartition- Yes upload");
	        String [] splitInput = bwString.split("\\s+");
	        int [] downlinks = new int [splitInput.length];
	        for (int i=0; i< splitInput.length; i++)
	        	downlinks[i] = Integer.parseInt(splitInput[i]);
	     	 int W = 0 ;//sum downlinks
	     	 for (int i =0; i<numPartitions; i++)
	     	    W += downlinks[i];
	     	 res =(key.hashCode() & Integer.MAX_VALUE) % W;
	     	 int optPartit = 0;
	     	 int partitionIndicator = downlinks[0];
	     	 while (res > partitionIndicator)
	     	   {
	     	      optPartit++;        
	     		  partitionIndicator += downlinks[optPartit];
	     	    }//while
	     	 res = optPartit;
	     	 }//else

	  	 
		  return res;
	   }//fun getPartition
	  		
	}//class newPartitionerClass
	  

	public static void main(String[] args) throws Exception 
	{// input_1 input_2 input_3 output inputsplitSize numReducers downlinkVec JobName
		Configuration conf = new Configuration();
		// # of mappers = size_input / split size [Bytes], split size=  max(mapreduce.input.fileinputformat.split.minsize, min(mapreduce.input.fileinputformat.split.maxsize, dfs.blocksize))
		conf.set("mapreduce.input.fileinputformat.split.minsize", args[4]); 
		conf.set("mapreduce.input.fileinputformat.split.maxsize", args[4]);
	    conf.set("mapreduce.map.log.level", "DEBUG");
		conf.set("bw", args[6]); // pass the downlink vector of partitions
		conf.set("ANum", "2"); // pass the table size
		conf.set("BNum", "3"); // pass the table size
		conf.set("CNum", "2"); // pass the table size
		Job job = Job.getInstance(conf, args[7]);
		job.setJarByClass(OneWayReg.class);
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class); // needed?
		job.setOutputFormatClass(TextOutputFormat.class); // needed?
		job.setNumReduceTasks(Integer.valueOf(args[5]));

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, XMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, YMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, ZMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.out.println("Downlinks list: " + args[6]);

		if (!job.waitForCompletion(true))
			System.exit(1);

	}// main

}// OneWay2 class
