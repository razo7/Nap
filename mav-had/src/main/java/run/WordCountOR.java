package run;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountOR extends Configured implements Tool {
	public static class wcMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public static final Log log = LogFactory.getLog(wcReducer.class);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}// while

		}// map
	}// MyMapper

	public static class wcReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public static final Log log = LogFactory.getLog(wcReducer.class);
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();

			result.set(sum);
			context.write(key, result);
		}// reduce
	}// reducer

	public static class newPartitionerClass extends 
	Partitioner<Text, IntWritable> implements
	org.apache.hadoop.conf.Configurable {
		int[] PartitionSize;
		int W = 0;// sum downlinks
		private static final Log LOG = LogFactory
				.getLog(newPartitionerClass.class);

		@Override
		public void setConf(Configuration conf) {
			String bwString_RM = "";
			String bwNodeString = conf.get("bwNodeString");
			//bwString_RM = conf.get("bw_RM");
			String PartitionSizeString = "";
			
				try {
					FileSystem fs = FileSystem.get(
							URI.create("hdfs://master:9000"), conf);
					Path hdfsPath = new Path(
							"/user/hadoop2/HDFS_fileFromHeartbeat");
					FSDataInputStream inputStream = fs.open(hdfsPath);
					// Classical input stream usage
					String out = IOUtils.toString(inputStream, "UTF-8");
					bwString_RM = out.toString();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (bwString_RM == null || bwString_RM.compareTo("master") == 0)
				LOG.info("OR_Change-getPartition- No upload-1");
			else {
				String[] NodesBw = bwNodeString.split("\\s+");
				String[] ReducerNodes = bwString_RM.split("\\s+");
				PartitionSize = new int[ReducerNodes.length];
				for (int i = 0; i < ReducerNodes.length; i++) {
					if (ReducerNodes[i].compareTo("master") == 0 )
						PartitionSize[i] = Integer.parseInt(NodesBw[0]);
					else if (ReducerNodes[i].compareTo("razoldslave1-len") == 0 )
						PartitionSize[i] = Integer.parseInt(NodesBw[1]);
					else
						PartitionSize[i] = Integer.parseInt(NodesBw[2]);
					PartitionSizeString += PartitionSize[i] + " ";
					W += PartitionSize[i];
				}//for
				LOG.info("OR_Change-getPartition- Yes upload\n" + bwString_RM
						+ " " + bwNodeString + " PartitionSizeString- " +PartitionSizeString);
			}// else
		}// setConf

		@Override
		public Configuration getConf() {
			return null;
		}

		// important for partitioning tuples with the same reducer ID to the
		// same destination(partition)
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) { 
			// read time
			int res = 0;
			if (W == 0)
				res = (key.hashCode() & Integer.MAX_VALUE) % numPartitions;

			else {// when we have the new allocation
				res = (key.hashCode() & Integer.MAX_VALUE) % W;
				int optPartit = 0;
				int partitionIndicator = PartitionSize[0];
				while (res > partitionIndicator) {
					optPartit++;
					partitionIndicator += PartitionSize[optPartit];
				}// while
				res = optPartit;
			}// else

			return res;
		}// fun getPartition


	}// class newPartitionerClass

	public int run(String[] args) throws Exception {
		// input output inputsplitSize numReducers NodedownlinkVec JobName rounds regularPartitionBit
		Configuration conf = getConf();
		// # of mappers = size_input / split size [Bytes], split size=
		// max(mapreduce.input.fileinputformat.split.minsize,
		// min(mapreduce.input.fileinputformat.split.maxsize, dfs.blocksize))
		conf.set("mapreduce.input.fileinputformat.split.minsize", args[2]);
		conf.set("mapreduce.input.fileinputformat.split.maxsize", args[2]);
		conf.set("mapreduce.map.log.level", "DEBUG");
		// conf.set("mapreduce.task.profile", "true");
		// conf.set("mapreduce.task.profile.reduces", "0-5");
		// conf.set("mapreduce.task.timeout", "1800000"); //30 minutes wait for
		// before killing the task
		conf.set("bwNodeString", args[4]); // pass the downlink vector of
											// partitions
		//System.setProperty("hadoop.home/dir", "/");
		String resPrint = loopRun(conf, args, 0);
		if (Integer.parseInt(args[7]) == 1)
		{
			String resPrint2 = loopRun(conf, args, 1);
			System.out.println("Update-getPartition: " + resPrint);
			System.out.println("WIthout Update-getPartition: " + resPrint2);
		}//if
		else 
			System.out.println("Update-getPartition " + resPrint);
		return (0);
	}

	public static String loopRun (Configuration conf, String [] args, int regularPartitionBit)
	{
		int rounds = Integer.parseInt(args[6]);
		int totalTime = 0;
		long[] elaspeJobTimeArr = new long[rounds];
		for (int i = 0; i < rounds; i++) 
		{
			try {
				if(regularPartitionBit == 1)
					elaspeJobTimeArr[i] = myRunJob(conf, args, String.valueOf(rounds + i), regularPartitionBit);
				else
					elaspeJobTimeArr[i] = myRunJob(conf, args, String.valueOf(i), regularPartitionBit);
			} catch (ClassNotFoundException | IOException
					| InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Job " + i + " took "
				+ ((elaspeJobTimeArr[i] / 1000) / 60) + " minutes and "
				+ ((elaspeJobTimeArr[i] / 1000) % 60) + " seconds");
			totalTime += elaspeJobTimeArr[i];
		}//for
		for (int i = 0; i < rounds; i++)
			System.out.println("Job " + i + " took "
				+ ((elaspeJobTimeArr[i] / 1000) / 60) + " minutes and "
				+ ((elaspeJobTimeArr[i] / 1000) % 60) + " seconds");
       String AvgRes = "Average Job took "
   			+ (((totalTime / rounds) / 1000) / 60) + " minutes and "
   			+ (((totalTime / rounds) / 1000) % 60) + " seconds";
		return AvgRes;
	}//loopRun
	
	public static long myRunJob(Configuration conf, String[] args, String index, int regularPartitionBit)
			throws ClassNotFoundException, IOException, InterruptedException {
		Job job = Job.getInstance(conf, args[5]);
		job.setJarByClass(WordCountOR.class);
		job.setMapperClass(wcMapper.class);
		if (regularPartitionBit == 0)
			job.setPartitionerClass(newPartitionerClass.class);
		job.setReducerClass(wcReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(Integer.valueOf(args[3]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(new Path(args[1]), index));
		System.out.println("Node downlinks list: " + args[4]);

		long start1 = new Date().getTime();
		if (!job.waitForCompletion(true))
			System.exit(1);
		return (new Date().getTime() - start1);

	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new WordCountOR(), args);
		System.exit(exitcode);
	}// main

}// wordcount1
