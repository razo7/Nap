package run;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import run.AcmMJSort.IndexReduceOneLoop;
import run.AcmMJSort.XMapper;
import run.AcmMJSort.YMapper;
import run.AcmMJSort.ZMapper;
import run.AcmMJSort.newPartitionerClass;
import examples.TextPair;

public class WordCountOR2 extends Configured implements Tool 
{
	public static class wcMapper extends Mapper<Object, Text, Text, IntWritable> 
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public static final Log log = LogFactory.getLog(wcReducer.class);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) 
			{
				word.set(itr.nextToken());
				context.write(word, one);
			}// while

		}// map
	}// MyMapper

	public static class wcReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public static final Log log = LogFactory.getLog(wcReducer.class);
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();
			result.set(sum);
			context.write(key, result);
		}// reduce
	}// reducer

	public static class newPartitionerClass extends Partitioner<Text, IntWritable> implements  org.apache.hadoop.conf.Configurable
	{		
		  int [] PartitionSize;
		  int [][] indexReducerBySlave;
		  int [] countReducerBySlave;
		//  private static float rangeFix;
	      private static int W = 0 ;//sum downlinks
		  private static final Log LOG = LogFactory.getLog(newPartitionerClass.class);
	
		  @Override
		    public void setConf (Configuration conf)
		    {
		     // int r = Integer.parseInt(conf.get("r"));//num_reducers
		      String bwString_RM = "";
			  String bwNodeString = conf.get("bwNodeString");
			  String NodeString = conf.get("NodeString"); //slave names
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
				  LOG.info("OR_Change-newPartitionerClass- Successful conf.get\n"+ bwString_RM + " " +bwNodeString);
		    	}
		    	 if (bwString_RM == null)
			    	 LOG.info("OR_Change-newPartitionerClass- No upload-1");
		    	 else
		    	 {
		    		 String [] NodesBw = bwNodeString.split("\\s+");
		    		 String [] ReducerNodes = bwString_RM.split("\\s+");
		    		 String [] slaveNames = NodeString.split("\\s+");
		 	         PartitionSize = new int [ReducerNodes.length];
		 	         LOG.info("OR_Change-newPartitionerClass- Yes upload\n"+ bwString_RM + "\nPartitionSize- " +bwNodeString + "\nslaveNames- " + NodeString);
		 	         indexReducerBySlave = new int [NodesBw.length][ReducerNodes.length];
		 	         countReducerBySlave = new int [NodesBw.length];
		 	        for (int i=0; i< NodesBw.length; i++)
		 	        	countReducerBySlave[i] = 0;
		 	         for (int i=0; i< ReducerNodes.length; i++)
		 	        	{
		 	        	for (int j=0; j< NodesBw.length; j++)
		 	        	{
		 	        		if (ReducerNodes[i].equals(slaveNames[j]))
		 	        		{
		 	        			indexReducerBySlave[j][countReducerBySlave[j]] = i;
		 	        			countReducerBySlave[j]++;
			 	        		//PartitionSize[i] = Integer.parseInt(NodesBw[j]);
			 	        		continue;
		 	        		}
		 	        	}
		 	        	 //W += PartitionSize[i];
		 	        	}
		 	        for (int j=0; j< NodesBw.length; j++)
		 	        {
		 	        	PartitionSize[j] = Integer.parseInt(NodesBw[j]);
		 	        	W += PartitionSize[j];
		 	        }
		    	 }//else
		    	 LOG.info("OR_Change-newPartitionerClass- W = " + W );
		    }//setConf
		    
		    @Override
		    public Configuration getConf()
		    {
		    	return null;
		    }
		
		  //important for partitioning tuples with the same reducer ID to the same destination(partition)
	    @Override
	    public int getPartition(Text key, IntWritable value, int numPartitions)
	    {	
	     int res=0;
	     //int keyRes = (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	  	 if (W == 0)
	  		 //res = (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	  		res = (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	  	 else
	  	 {//when we have the new allocation
	  		 
	  		//res = (key.getFirst().hashCode() & Integer.MAX_VALUE) % W; 
	  		//res = Math.round(keyRes*rangeFix) ;//extend to W values WHY?
	  		 int oldres = (key.hashCode() & Integer.MAX_VALUE) % W;
	  		 
	  		 int optPartit = 0;
	     	 int partitionIndicator = PartitionSize[optPartit];
	       	 while (partitionIndicator == 0 || oldres > partitionIndicator)// if PartitionSize[optPartit] is zero
	       		 // we skip because we should try to avoid use him
	     	   {
	     	      optPartit++;
	     		  partitionIndicator += PartitionSize[optPartit];
	     	    }//while
	       	 int slaveIndex = optPartit; //indexOfSelectedSlave
	       	 int toReducerIndex = (key.hashCode() & Integer.MAX_VALUE) % countReducerBySlave[slaveIndex];
	     	 res = indexReducerBySlave[slaveIndex][toReducerIndex];
	     /*	 LOG.info("Ultimate Test- key = " + key + ", oldres = " + oldres + ", slaveIndex = " + slaveIndex +
	     			 ", countReducerBySlave[slaveIndex] = " + countReducerBySlave[slaveIndex] + ", toReducerIndex = " + toReducerIndex + 
	     			 ", res = " + res );
	     			 */
	     }//else
	  	return res;
	   }//fun getPartition
	}//class newPartitionerClass

	 public int run (String[] args) throws Exception
	    {// input output inputsplitSize num_reducers slave_names downlinkVec JobName rounds
			Configuration conf = getConf();
			// # of mappers = size_input / split size [Bytes], split size=  max(mapreduce.input.fileinputformat.split.minsize, min(mapreduce.input.fileinputformat.split.maxsize, dfs.blocksize))
			conf.set("mapreduce.input.fileinputformat.split.minsize", args[2]); 
			conf.set("mapreduce.input.fileinputformat.split.maxsize", args[2]);
			conf.set("mapreduce.map.log.level", "DEBUG");
			//conf.set("mapreduce.task.profile", "true");
			//conf.set("mapreduce.task.profile.reduces", "0-5");
			conf.set("mapreduce.task.timeout", "900000"); //15 minutes wait for before killing the task
			conf.set("NodeString", args[4]); // pass the slave names
			conf.set("bwNodeString", args[5]); // pass the downlink vector of partitions
			System.setProperty("hadoop.home/dir", "/");
			
			int rounds = Integer.parseInt(args[7]);
			//conf.set("r", args[3]); // pass the num_reducers to newPartitioner Class
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
		
	public static long myRunJob(Configuration conf, String[] args, String index) throws ClassNotFoundException, IOException, InterruptedException 
	{
		Job job = Job.getInstance(conf, args[6]);
		job.setJarByClass(WordCountOR2.class);
		job.setMapperClass(wcMapper.class);
		job.setPartitionerClass(newPartitionerClass.class);
		job.setReducerClass(wcReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(Integer.valueOf(args[3]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(new Path(args[1]), index));
		System.out.println("Num reducers: " + args[3] + "\nSlaves list: " + args[4] + "\ndownLink: " + args[5]);
		long start1 = new Date().getTime();
		if (!job.waitForCompletion(true))
			System.exit(1);
		return (new Date().getTime() - start1);

	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new WordCountOR2(), args);
		System.exit(exitcode);
	}// main

}// wordcount1
