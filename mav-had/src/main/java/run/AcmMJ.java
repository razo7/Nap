package run;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AcmMJ extends Configured implements Tool{
	public static class XMapper extends Mapper<LongWritable, Text, Text, Text> 
	{//article
		private int ANum;
		private int BNum;		
		private static final Log LOG = LogFactory.getLog(XMapper.class);
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
				if (splitInput.length == 2)
				{
				Text article_id = new Text(splitInput[0]);
				Text publication_id = new Text(splitInput[1]);
				Text opString = new Text("X" + article_id + "," + publication_id);
				String a1 = String.valueOf(1 + (splitInput[0].hashCode() & Integer.MAX_VALUE % ANum));
				
				for (int i = 1; i <= BNum; i++) 
				{
					String b1 = String.valueOf(i);
					reducerIndex = a1 + b1;
		//			LOG.info("Logging Z-article keys! : " +reducerIndex);
					context.write(new Text(reducerIndex), opString);
				}
		//		LOG.info("Logging Z-article value! : " +opString);
				}//if
		//		else
		//			LOG.info("Logging Z-article problem! missing attribute");
			}//map

			}// XMapper Class

	public static class YMapper extends Mapper<LongWritable, Text, Text, Text> 
	{//article_author
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
			if (splitInput.length == 2)
			{
			Text a = new Text(splitInput[0]);
			Text b = new Text(splitInput[1]);
			Text opString = new Text("Y" + a + "," + b);
			String a1 = String.valueOf(1 + (splitInput[0].hashCode() & Integer.MAX_VALUE % ANum));
			String b1 = String.valueOf(1 + (splitInput[1].hashCode() & Integer.MAX_VALUE % BNum));
			context.write(new Text( a1 + b1), opString);
	//		LOG.info("Logging Y-article_author value! : " +opString);
			}//if
	//		else
	//			LOG.info("Logging Y-article_author problem! missing attribute");
		}// map

	}// YMapper Class

	public static class ZMapper extends Mapper<LongWritable, Text, Text, Text> 
	{//persons
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
			if (splitInput.length == 3)
			{
			Text person = new Text(splitInput[0]);
			Text Fname = new Text(splitInput[1]);
			Text Lname = new Text(splitInput[2]);

			Text opString = new Text("Z" + person + "," + Fname +"," + Lname );
			String b1 = String.valueOf(1 + (splitInput[0].hashCode() & Integer.MAX_VALUE % BNum));
			for (int i = 1; i <= ANum; i++) 
			{
				String a1 = String.valueOf(i);
				reducerIndex = a1 + b1;
		//		LOG.info("Logging X-persons keys! : " +reducerIndex);
				context.write(new Text(reducerIndex), opString);
			}
		//	LOG.info("Logging X-persons value! : " +opString);
			}//if length
		//	else
		//	LOG.info("Logging X-persons problem! missing attribute");
		}// map
	}// ZMapper Class

	public static class ReduceIndex extends	Reducer<Text, Text, Text, Text> 
	{
		private static final Log LOG = LogFactory.getLog(ReduceIndex.class);
		List<Text> RelX;
		List<Text> RelY;
		List<Text> RelZ;
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{

			RelX = new ArrayList<Text>();
			RelY = new ArrayList<Text>();
			RelZ = new ArrayList<Text>();
			List <Text> Rel_mid = new ArrayList<Text>();
			int index = 0;
			int save_index = 0;
			int matchKey = 0;
			String last_key = "";
			String last_key_break_loop = "";
		
			for (Text val :values ) 
			{
				String relationValue = val.toString();
				String finalVal = relationValue.substring(1, relationValue.length());
				if (relationValue.indexOf('X') == 0) 
					RelX.add(new Text(finalVal));//persons
				else if (relationValue.indexOf('Y') == 0) 
					RelY.add(new Text(finalVal));//article_author
				else 
					RelZ.add(new Text(finalVal));//article
			}// FOR
			//Sort?
			Collections.sort(RelX);
			Collections.sort(RelY);
			Collections.sort(RelZ);
			/*LOG.info("Table X- " + RelX.size() );
			for (Text t : RelX)
				LOG.info(t);
			LOG.info("Table Y- " + RelY.size());
			for (Text t : RelY)
				LOG.info(t);
			LOG.info("Table Z- " + RelZ.size());
			for (Text t : RelZ)
				LOG.info(t); */
		
			
			LOG.info("before 2 for");
			for (Text x : RelX) 
			{//persons
				String[] Xtemp = x.toString().split(",");
				if (! last_key.equalsIgnoreCase(Xtemp[0]) && matchKey > 0)
					RelY = RelY.subList(save_index + matchKey, RelY.size());
				matchKey = 0;
				save_index = 0;
			//	LOG.info("Logging loop reducer  X: " + key + " person_id: " +Xtemp[0]);
				for (Text y : RelY) 
				{
					String[] Ytemp = y.toString().split(",");
//					LOG.info("Logging loop reducer  Y: " + key+ " X-article_id: " +Xtemp[0] + 
//							", Y-article_id: " + Ytemp[0] + ", X-publication_id: " +Xtemp[1] + ", Y-person_id: " + Ytemp[1]);
					if (Xtemp[0].equalsIgnoreCase(Ytemp[0])) 
					{
						Rel_mid.add(new Text(Ytemp[1]+ "," + Xtemp[0]+ "," + Xtemp[1]));
						if (matchKey == 0)
						{
							save_index = index;
							last_key = Ytemp[0];
							last_key_break_loop = Ytemp[0];
						}//move forward the list
						matchKey++;
					}// if
					index++;
					if (! last_key_break_loop.equalsIgnoreCase("") && ! last_key_break_loop.equalsIgnoreCase(Ytemp[0]) )
						break;
					
					}// for
				//if (index == RelY.size() && matchKey == 0)
				//	break;
				last_key_break_loop = "";
				index = 0;
			}// for
			index = 0;
			save_index = 0;
			matchKey = 0;
			last_key = "";
			last_key_break_loop = "";
			
				Collections.sort(Rel_mid);
			/*	LOG.info("Table Rel_mid- " + Rel_mid.size());
				for (Text t : Rel_mid)
					LOG.info(t);
				LOG.info("before 1 for"); */
				for (Text z : RelZ) 
				{
					String[] Ztemp = z.toString().split(",");
					if (! last_key.equalsIgnoreCase(Ztemp[0]) && matchKey > 0)
						Rel_mid = Rel_mid.subList(save_index + matchKey, Rel_mid.size());
					matchKey = 0;
					save_index = 0;
//					LOG.info("Logging loop reducer  Z: " + key.getreducerIndex() + " person_id: " +Xtemp[0] + ", Y-person_id: " + Ytemp[0] +", Y-article_id: " +Ytemp[1] + ", Z-article_id: " + Ztemp[0] );
					for (Text mid : Rel_mid)
					{
						String[] Midtemp = mid.toString().split(",");
//						LOG.info("Logging loop reducer  Z: " + key + " Z-person_id: " + Ztemp[0] + ", Midtemp-person_id: " +Midtemp[0] + 
//								 ", Midtemp-article_id: " +Midtemp[1] + ", Midtemp-publicaiton_id: " + Midtemp[2] );
						
						if (Midtemp[0].equalsIgnoreCase(Ztemp[0])) 
						{
							context.write(key, new Text(Ztemp[0]+ " " + Ztemp[1]+ " " + Ztemp[2]+ " " + Midtemp[1] + " " + Midtemp[2]));
							if (matchKey == 0)
							{
								save_index = index;
								last_key = Midtemp[0];
								last_key_break_loop = Midtemp[0];
							}//move forward the list
							matchKey++;
						}//if
						index++;
						if (! last_key_break_loop.equalsIgnoreCase("") && ! last_key_break_loop.equalsIgnoreCase(Midtemp[0]) )
							break;
					}//for
					//if (index == Rel_mid.size() && matchKey == 0)
					//	break;
					last_key_break_loop = "";
					index= 0;
					
				}// for
		LOG.info("Logging reducer : " +key);
		}// reduce		
	}// ReduceIndex Class


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
			    conf.set("mapreduce.task.timeout", "900000"); //15 minutes wait for before killing the task
			    conf.set("bwNodeString", args[7]); // pass the downlink vector of partitions
			    String [] splitInput = args[5].split("\\s+");
				conf.set("ANum", splitInput[0]); // pass the table size
				conf.set("BNum", splitInput[1]); // pass the table size
//				System.setProperty("hadoop.home/dir", "/");
				int rounds = Integer.parseInt(args[9]);
				long [] elaspeJobTimeArr = new long [rounds]; 
				int totalTime = 0;			
				for (int i=0; i< rounds; i++)
				{
					elaspeJobTimeArr[i] = myRunJob(conf, args, String.valueOf(i));	
					System.out.println("Job "+ i +" took "+ ((elaspeJobTimeArr[i] /1000) /60) + " minutes and " +((elaspeJobTimeArr[i] /1000)%60) + " seconds");	
					totalTime += elaspeJobTimeArr[i];
				}
				for (int i=0; i< rounds; i++)
					System.out.println("Job "+ i +" took "+ ((elaspeJobTimeArr[i] /1000) /60) + " minutes and " +((elaspeJobTimeArr[i] /1000)%60) + " seconds");	
				 
				System.out.println("Average Job took "+ (((totalTime /rounds)/1000) /60) + " minutes and " + (((totalTime /rounds)/1000)%60) + " seconds");		
				return(0);	  
	
    }
	
	public static long myRunJob (Configuration conf, String [] args, String index) throws ClassNotFoundException, IOException, InterruptedException
	{
		Job job = Job.getInstance(conf, args[8]);
		job.setJarByClass(AcmMJ.class);
		job.setPartitionerClass(newPartitionerClass.class);
		job.setReducerClass(ReduceIndex.class);
		job.setInputFormatClass(TextInputFormat.class); // needed?d
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
		int exitcode = ToolRunner.run(new AcmMJ(), args);
		System.exit(exitcode);
	}// main
	

}// OneWayUp class
