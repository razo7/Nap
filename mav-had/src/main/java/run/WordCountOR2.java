package run;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import run.AcmMJSort.XMapper;
import run.AcmMJSort.YMapper;
import run.AcmMJSort.ZMapper;

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
		  private static int [] slaveSize; // proportion of slaves BW
		  private static int [][] reducerIndicesPerSlave; // indices of reducers in reducerArrayHDFS according to slave
		  private static int [] counterReducers; // counter of reducers per slave
		  private static int [] reducersSlaveIndices; // mapping from all the slaves to working slaves in reduce
	      private static int W = 0 ; //sum of downlinks (slaveSize)
		  private static final Log LOG = LogFactory.getLog(newPartitionerClass.class);
	  @Override
		    public void setConf (Configuration conf)
		    {
			  int i,j;
		      String mapperrArrayHDFS = "";//mappersLocationString
		      String reducerArrayHDFS = "";//reducersLocationString
			  String bwNodeString = conf.get("bwNodeString");
			  String NodeString = conf.get("NodeString"); //slave names
			  String [] slavesBW = bwNodeString.split(":"); // array of slave's downlinks per slave
			  String [] slaveNames = NodeString.split("\\s+"); // array of slave names
			  int [] counterMappers = new int [slaveNames.length];; // counter of mappers per slave
			  int [] selectedBW = new int [slaveNames.length];
			  counterReducers = new int [slaveNames.length];
			  slaveSize = new int [slaveNames.length];
			  reducersSlaveIndices= new int [slaveNames.length];
	 	      for (i=0; i< slaveNames.length; i++)
	 	          {// initialize the arrays to zero
	 	    	      counterMappers[i] = 0;
	 	    	      counterReducers[i] = 0;
	 	    	      selectedBW[i] = 0;
	 	          }
			  String infoIndices = ""; //indices of reducers for debugging
			  String infoCounters = ""; //counter of reducers for debugging
			  String infoSlavesIndices = ""; //Mapping indices of reducers for debugging
			  String infoSlavesBW = ""; //Mapping indices of reducers for debugging
			  while (reducerArrayHDFS == "")
			  {
		    	try {
					FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), conf);
					//mappers
					Path hdfsPathMappers = new Path("/mappersLocations");
		        	FSDataInputStream inputStreamM = fs.open(hdfsPathMappers);
			        String outM = IOUtils.toString(inputStreamM, "UTF-8");//Classical input stream usage
			        mapperrArrayHDFS = outM.toString(); 
				    
					//reducers
					Path hdfsPathReducers = new Path("/reducersLocations");
		        	FSDataInputStream inputStreamR = fs.open(hdfsPathReducers);
			        String outR = IOUtils.toString(inputStreamR, "UTF-8");//Classical input stream usage
			        reducerArrayHDFS = outR.toString(); 
				    }//try
		    	catch (IOException e) { e.printStackTrace(); }
		    	
		    	if (reducerArrayHDFS == "")
		    		 LOG.info("OR_Change-newPartitionerClass- No upload-1");
			   }//while
			         String [] mappersSlaves = mapperrArrayHDFS.split("\\s+"); // array of mapper's slaves according to the their ID
			         String [] reducerSlaves = reducerArrayHDFS.split("\\s+"); // array of reducer's slaves according to the their ID
		    		 LOG.info("OR_Change-newPartitionerClass- Yes upload\nMappers: "+ mapperrArrayHDFS + "\nReducers: " + reducerArrayHDFS + "\nPartitionSize- " + bwNodeString + "\nslaveNames- " + NodeString);
		    		 //count mappers to pick the right BW array
		    		 String infoMappersBW = "";
		    		 String infoMappersAmount = "";
		    		 for (i=0; i< mappersSlaves.length; i++)
		 	        	{
		 	        	 for (j=0; j< slaveNames.length; j++)
		 	        	 {
		 	        		if (mappersSlaves[i].equals(slaveNames[j]))
		 	        		{
		 	        			counterMappers[j]++;
			 	        		continue;
		 	        		}//if
		 	        	 }//for
		 	        	}//for
		    		
		    		 for (i=0; i< slaveNames.length; i++)
		 	        	{
		    			 for (j=0; j< slaveNames.length; j++)
			 	        	{
		    			     String [] slaveBW = slavesBW[j].split("\\s+"); // array of slave's downlinks of slave 	        		 
		 	        	     selectedBW[i] += (Integer.parseInt(slaveBW[i]) * counterMappers[i]);
			 	        	}
		    			 selectedBW[i] =  (selectedBW[i] /slaveNames.length);
		    			 infoMappersAmount += String.valueOf(counterMappers[i]) + " ";
		    			 infoMappersBW += String.valueOf(selectedBW[i]) + " ";
		 	        	}
		    		 LOG.info("OR_Change-newPartitionerClass-mappers \ninfoMappersAmount: "+ infoMappersAmount + "\ninfoMappersBW: " + infoMappersBW);
		 	         reducerIndicesPerSlave = new int [slaveNames.length][reducerSlaves.length];
		 	      //count reducers to pick the right allocation of data between slaves
		 	         for (i=0; i< reducerSlaves.length; i++)
		 	        	{
		 	        	 for (j=0; j< slaveNames.length; j++)
		 	        	 {
		 	        		if (reducerSlaves[i].equals(slaveNames[j]))
		 	        		{
		 	        			reducerIndicesPerSlave[j][counterReducers[j]] = i;
		 	        			counterReducers[j]++;
			 	        		continue;
		 	        		}//if
		 	        	 }//for
		 	        	}//for
		 	         j = 0;
		 	        for (i = 0; i < selectedBW.length; i++)
		 	        {
		 	        	if (counterReducers[i] > 0)
		 	        	{
				 	         /// add blacklist code
		 	        		slaveSize[j] = selectedBW[i];
		 	        		infoSlavesBW = infoSlavesBW + selectedBW[i] + ", ";
		 	        		W += slaveSize[j];
		 	        		reducersSlaveIndices[j] = i; 
		 	        		j++;
		 	        		
		 	        	} //if
		 	        }//for
		 	        // just for debugging with LOG
		 	        for (i=0; i<selectedBW.length; i++)
		 	        {
		 	        	infoCounters = infoCounters + String.valueOf(counterReducers[i]) + ", ";
		 	        	if (counterReducers[i] > 0)
		 	        	     infoSlavesIndices = infoSlavesIndices + "(" + String.valueOf(i) + ", " + String.valueOf(reducersSlaveIndices[i]) + "), ";
		 	        	infoIndices +="\n";
		 	        	for (j=0; j< counterReducers[i]; j++)
		 	        	     infoIndices = infoIndices + String.valueOf(reducerIndicesPerSlave[i][j]) + " ";
		 	            		 	      
		 	        }
		   // 	 LOG.info("OR_Change-newPartitionerClass- W = " + W + ", slaves Size\n" + infoSlavesBW + "\nCounters\n" + infoCounters + "\nIndices:" + infoIndices +"\nreducersSlaveIndices:\n" + infoSlavesIndices );
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
	     int res = 0; //the default partition
	  	 if (W == 0)
	  		res = (key.hashCode() & Integer.MAX_VALUE) % numPartitions; // when W=0 we partition the tuples evenly
	  	 else
	  	 {//when we have the new allocation
	  		 int oldres = (key.hashCode() & Integer.MAX_VALUE) % W; // when W>0 we partition the tuples according to slaveSize
	  		 int slaveIndex = 0; // index of slave
	     	 int partitionIndicator = slaveSize[slaveIndex];
	       	 while (partitionIndicator == 0 || oldres >= partitionIndicator)// if PartitionSize[optPartit] is zero
	       	   { // we skip because we should try to avoid use him
	       		  slaveIndex++;
	     		  partitionIndicator += slaveSize[slaveIndex];
	     	   }//while
	       	 int realSlaveIndex = reducersSlaveIndices[slaveIndex];
	      	// LOG.info("my key - " + key + ", oldres= " +  String.valueOf(oldres) + ", slaveIndex= " + String.valueOf(slaveIndex) + ", partitionIndicator= " + String.valueOf(partitionIndicator) + ", slave= " + String.valueOf(realSlaveIndex) );
	         int toReducerIndex = (key.hashCode() & Integer.MAX_VALUE) % counterReducers[realSlaveIndex];
	       	 res = reducerIndicesPerSlave[realSlaveIndex][toReducerIndex];
	     //  LOG.info("Ultimate Test- key = " + key + ", oldres = " + oldres + ", slaveIndex = " + slaveIndex +  ", countReducerBySlave[slaveIndex] = " + countReducerBySlave[slaveIndex] + ", toReducerIndex = " + toReducerIndex + ", res = " + res );
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
