package run;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import examples.TextPair;

public class AcmMJSort extends Configured implements Tool{
	public static class XMapper    extends Mapper<LongWritable, Text, TextPair, Text> 
	{//article
		private int ANum;
		private int BNum;
		private static final Log LOG = LogFactory.getLog(XMapper.class);
		@Override 
		protected void setup(Context context) 
		{
		        Configuration c = context.getConfiguration();
		        ANum = Integer.parseInt(c.get("ANum"));//Article_id
		        BNum = Integer.parseInt(c.get("BNum"));//Person_id
		} 
		  @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String reducerIndex;
			String line = value.toString();
			String[] splitInput = line.split("\\t");
			if (splitInput.length == 2)
			{
			String a1 = String.valueOf(splitInput[0].hashCode() & Integer.MAX_VALUE % ANum);
			for (int i = 0; i < BNum; i++) 
			{
				String b1 = String.valueOf(i);
				reducerIndex = a1 + b1;
//				LOG.info("Logging X-article keys! : " +reducerIndex);
				context.write(new TextPair (reducerIndex,"X" + splitInput[0]), new Text (splitInput[1]) );		
			}
			//LOG.info("Logging X-article value! : " +opString);
			}//if 
		}// map
	}// XMapper Class

	public static class YMapper    extends Mapper<LongWritable, Text, TextPair, Text>
	
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
			String a1 = String.valueOf(splitInput[0].hashCode() & Integer.MAX_VALUE % ANum);
			String b1 = String.valueOf(splitInput[1].hashCode() & Integer.MAX_VALUE % BNum);
			context.write(new TextPair (a1+b1,"Y" + splitInput[0]), new Text (splitInput[1]));
			//				LOG.info("Logging Y-article_author value! : " + line);
			}//if
		}// map
	}// YMapper Class

	public static class ZMapper    extends Mapper<LongWritable, Text, TextPair, Text>
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
			String line = value.toString();
			String[] splitInput = line.split("\\t");
			if (splitInput.length == 3)
			{
				String b1 = String.valueOf(splitInput[0].hashCode() & Integer.MAX_VALUE % BNum);
				for (int i = 0; i < ANum; i++) 
				{
					String a1 = String.valueOf(i);
					//LOG.info("Logging Z-persons keys! : " +reducerIndex);
					context.write(new TextPair (a1+b1,"Z" + splitInput[0]), new Text (splitInput[1] + " " + splitInput[2]));
				}
			//LOG.info("Logging Z-persons value! : " +opString);
			}//if 
		}//map
	}// ZMapper Class

	public static class IndexReduceOneLoop extends Reducer<TextPair, Text, Text, Text> 
	{
		private static final Log LOG = LogFactory.getLog(IndexReduceOneLoop.class);
	
		@Override
		public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{

			List <Text> RelX = new ArrayList<Text>();
			List <Text> Rel_mid = new ArrayList<Text>();
			int index = 0;
			int save_index = 0;
			int matchKey = 0;
			String last_key = "";
		    boolean startZ = true;
			
			if (key.getSecond().toString().charAt(0) != 'X')
			{//if there is not a X row, then we can't perform a multiway join
				//				LOG.info("Logging reducer first value is not from X! " + key.getFirst() +" value: " + key.getSecond());
				return ;
			}
			
			for (Text val :values ) 
			{
				String tempKey = key.getSecond().toString();
				char table = tempKey.charAt(0);
				String keyJoin = tempKey.substring(1, tempKey.length());
				if (table == 'X') 
				{//article
					
						RelX.add(new Text(keyJoin + "," + val));
				}// if- article
				else if (table == 'Y')
			{//article_author
					
					if (matchKey > 0 && !last_key.equalsIgnoreCase(keyJoin) )
						RelX = RelX.subList(save_index + matchKey, RelX.size());
					matchKey = 0;
					index = 0;
					save_index = 0;	
					for (Text x : RelX) 
				
					{//article
						last_key = keyJoin;
						String[] Xtemp = x.toString().split(",");
//						LOG.info("Logging loop reducer  Y: " + keyJoin + " "  + val + " X: " + Xtemp[0] + "," + Xtemp[1] + " matchKey- " + matchKey);
						if (Xtemp[0].equalsIgnoreCase(keyJoin))
						{
							Rel_mid.add(new Text(val + "," + Xtemp[0]+ "," + Xtemp[1]));
							if (matchKey == 0)
								save_index = index;//move forward the list
							matchKey++;
						}// if
						else if (matchKey > 0)
							break;
						index++;
					}// for	
			}// if- article_author
				else 
				{//persons
					
						if (startZ)
						{//sort
						/*	 LOG.info("Before sortedArray! "  + key.getFirst() + " with size: " + RelX_Y.size() ); for (TextPair x_y : RelX_Y) LOG.info("x_y: " + x_y); */
							Collections.sort(Rel_mid);
							startZ = false;
//							LOG.info("sortedArray! "  + key.getFirst() + " with size: " + Rel_mid.size() ); 
//							for (Text mid : Rel_mid) LOG.info("mid: " + mid);
							matchKey = 0;
						}
						if (matchKey > 0 && !last_key.equalsIgnoreCase(keyJoin) )
							Rel_mid = Rel_mid.subList(save_index + matchKey, Rel_mid.size());
						matchKey = 0;
						index = 0;
						save_index = 0;
						for (Text mid : Rel_mid) 
						{//article-author & article
							last_key = keyJoin;	
							String[] Midtemp = mid.toString().split(",");
//							LOG.info("Logging loop reducer  Z: " + keyJoin + " "  + val + " midTemp: " + Midtemp[0] + "," + Midtemp[1] + " matchKey- " + matchKey);
							if (Midtemp[0].equalsIgnoreCase(keyJoin)) 
								{
								context.write(key.getFirst(), new Text(keyJoin + " " + val + " " + Midtemp[1] + " " + Midtemp[2]));
								if (matchKey == 0)
									save_index = index;//move forward the list
								matchKey++;
								}// if
							else if (matchKey > 0)
								break;
							index++;
						}// for	
				}//else- persons
			}// for			
			LOG.info("Logging reducer : " +key);
		}// reduce
	}// IndexReduceOneLoop Class

	public static class SQLReduce extends 	Reducer<TextPair, Text, Text, Text> 
	{
		private static final Log LOG = LogFactory.getLog(SQLReduce.class);
		public static Connection	connection = null;
		public static Statement statement = null;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			 try 
			  {	
				  Class.forName("com.mysql.jdbc.Driver").newInstance();
				  connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/acm_ex", "root", "root");
				  statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
				 // LOG.info("SQL-  connection: " + connection + " statement: " + statement);
			  }
			 catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) 
			  { e.printStackTrace(); }
		}
		
		@Override
		public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{	
			  try 
			  {	
				  //create 3 tables names
				  String groupKey = key.getFirst().toString();//used for excluding the tables between reducers on the same physical computer
				  String xTable = "x_article_" + groupKey;
				  String yTable = "y_article_author_" + groupKey;
				  String zTable = "z_persons_" + groupKey;
				  String x_yRes = "x_yRes" + groupKey;
		          String final_res = "final_res_" + groupKey;
				 // statement.executeUpdate("drop table if exists "+ xTable + "," + yTable + "," + zTable + "," + x_yRes + "," + final_res );//clear old tables
				  statement.executeUpdate("CREATE temporary TABLE "+ xTable + " (article_id CHAR(20), publication_id CHAR(20)) ");
			      statement.executeUpdate("CREATE temporary TABLE "+ yTable + " (article_id CHAR(20), person_id CHAR(20)) ");
		          statement.executeUpdate("CREATE temporary TABLE "+ zTable + " (person_id CHAR(20), first_name CHAR(250), last_name CHAR(250)) ");
		          String xres = "insert into "+ xTable +" values ";
		          String yres = "insert into "+ yTable +" values ";
		          String zres = "insert into "+ zTable +" values ";
		          
		          for (Text val :values ) //load values to strings 
		        	  {
		        	  String relationValue = key.getSecond().toString();
		        	  String keyjoin = relationValue.substring(1, relationValue.length());
		        	  if (relationValue.indexOf('X') == 0) 
		        	//	  statement.executeUpdate("insert into "+ xTable +" values ('" + keyjoin + "','" + val.toString() + "')");//article
		        		  xres += "('" + keyjoin + "','" + val.toString() + "'),";//article
		        	  else if (relationValue.indexOf('Y') == 0) 
		        		  //statement.executeUpdate("insert into "+ yTable +" values ('" + keyjoin + "','" + val.toString() + "')");//article-author
		        		  yres += "('" + keyjoin + "','" + val.toString() + "'),";
		        	  else 
		        	     {
		        		     String [] Val = val.toString().split("\\s+");
		        		     //statement.executeUpdate("insert into "+ zTable +" values ('" + keyjoin + "','" + Val[0] + "','" + Val[1] + "')");//persons
		        		     zres += "('" + keyjoin + "','" + Val[0] + "','" + Val[1] + "'),";
		        	     }//else
		        	  }// FOR
		          //insert data to tables
		          xres = xres.substring(0, xres.length() -1);
		          yres = yres.substring(0, yres.length() -1);
		          zres = zres.substring(0, zres.length() -1);
		          statement.executeUpdate(xres);//article
		          statement.executeUpdate(yres);//article-author
		          statement.executeUpdate(zres);//persons
		          
		          //LOG.info("Reducer with key -"+ key.getFirst() + ", start join");
				  int res1 =  statement.executeUpdate("CREATE temporary TABLE " + x_yRes + " (select person_id, "+ xTable + ".article_id, publication_id "
				      + "from "+ xTable + " inner join "+ yTable + " on " + xTable + ".article_id = " + yTable + ".article_id)");
				  //LOG.info("Reducer with key -"+ key.getFirst() + ", " + res1 + " firstJoinRows");
				  if (res1 > 0)
				  {
					  int res2 = statement.executeUpdate("CREATE temporary TABLE " + final_res + " (select "+ zTable + ".person_id, first_name, last_name, article_id, publication_id "
					  + "from "+ zTable + " inner join " + x_yRes + " on " + zTable + ".person_id = " + x_yRes + ".person_id)");
				   //   LOG.info("Reducer with key -"+ key.getFirst() + ", finish join and start to write, " + res2 + " secondJoinRows");
				      if (res2 > 0)
				      {
 				    	  ResultSet rs = statement.executeQuery("SELECT * FROM " + final_res);
				    	//  LOG.info("Reducer with key -"+ key.getFirst() + ", write ");
				    	  while (rs.next())
				    		  context.write(key.getFirst(), new Text(rs.getString("person_id")+ "\t" + rs.getString("first_name") +
				    				  "\t" + rs.getString("last_name") + "\t" + rs.getString("article_id") + "\t" + rs.getString("publication_id") ));
				      }//if-res2
				 }//if-res1
				//  statement.executeUpdate("drop table if exists "+ xTable + "," + yTable + "," + zTable + "," + x_yRes + "," + final_res );//clear old tables
				  LOG.info("Reducer with key -"+ key.getFirst());
			}//try
			catch (SQLException  e) { e.printStackTrace(); }
		}//reduce
		 protected void cleanup(Context context ) throws IOException, InterruptedException 
		 {
				try { statement.close(); connection.close(); }
				catch (SQLException e) { e.printStackTrace(); }
		 }//cleanup
	}//SQLReduce
	
	public static class newPartitionerClass extends Partitioner<TextPair, Text> implements  org.apache.hadoop.conf.Configurable
	
	{		
		  int [] PartitionSize;
		  private static int ANum;
		  private static float rangeFix;
	      private static int W = 0 ;//sum downlinks
		  private static final Log LOG = LogFactory.getLog(newPartitionerClass.class);
	
		  @Override
		    public void setConf (Configuration conf)
		    {
		      ANum = Integer.parseInt(conf.get("ANum"));//Article_id
		      int r = Integer.parseInt(conf.get("r"));//Article_id
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
		 	         for (int i=0; i< ReducerNodes.length; i++)
		 	        	{
		 	        	 if (ReducerNodes[i].equals(slaveNames[0]) )
		 	        		PartitionSize[i] = Integer.parseInt(NodesBw[0]);
		 	        	 else if (ReducerNodes[i].equals(slaveNames[1]) )
			 	        	PartitionSize[i] = Integer.parseInt(NodesBw[1]);
		 	        	 else if (ReducerNodes[i].equals(slaveNames[2]) )
				 	        	PartitionSize[i] = Integer.parseInt(NodesBw[2]);
		 	        	 else
		 	        		PartitionSize[i] = Integer.parseInt(NodesBw[3]);
		 	        	 W += PartitionSize[i];
		 	        	}
		    	 }//else
		    	// prepareW();
		    	 rangeFix = (float)(W/r);
		    	 LOG.info("OR_Change-newPartitionerClass- W = " + W + ", ANum = " + ANum);
		    }//setConf
		    
		    @Override
		    public Configuration getConf()
		    {
		    	return null;
		    }
		/*    	    
		 public static void prepareW ()
			{
				int cand = (int) Math.floor(Math.sqrt(W));
				while (cand > 0)
				{
					if (W % cand == 0)
						break;
					cand--;
				}
				ANum2=cand;
			 }
		 */
		 public static int MJHashEqual (Text key)
		 {
			 String mykey = key.toString();
			 //if (isW)
				// return Character.getNumericValue(mykey.charAt(0)) * ANum2 + Character.getNumericValue(mykey.charAt(1));
			 return Character.getNumericValue(mykey.charAt(0)) * ANum + Character.getNumericValue(mykey.charAt(1)); 
			 
		 }//MJHashEqual
		/* public static int getNewKey (TextPair key, Text value)
		 {
			 String res;
			 String s = key.getSecond().toString();
			 char table = s.charAt(0);
			 String keyJoin = s.substring(1);
			 switch (table)
			 {
			 case 'X':
			     for (int i=0; i< ANum2; i++)
			     {
			    	 
			     }
				 res = 
				 break;
			 case 'Y':
				 break;
			default:
				break;			 
			 }
			return new Text(res);
			 context.write(new TextPair (reducerIndex,"X" + splitInput[0]), new Text (splitInput[1]) ); 
		 }
		 */
		  //important for partitioning tuples with the same reducer ID to the same destination(partition)
	    @Override
	    public int getPartition(TextPair key, Text value, int numPartitions)
	    {	
	     int res=0;
	     int keyRes = MJHashEqual(key.getFirst());
	  	 if (W == 0)
	  		 //res = (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	  		res = keyRes;
	  	 else
	  	 {//when we have the new allocation
	  		 
	  		//res = (key.getFirst().hashCode() & Integer.MAX_VALUE) % W; 
	  		res = Math.round(keyRes*rangeFix) ;//extend to W values
	  		
	  		 int optPartit = 0;
	     	 int partitionIndicator = PartitionSize[optPartit];
	       	 while (partitionIndicator == 0 || res > partitionIndicator)// if PartitionSize[optPartit] is zero
	       		 // we skip because we should try to avoid use him
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
			    conf.set("NodeString", args[6]); // pass the slave names
			    conf.set("bwNodeString", args[7]); // pass the downlink vector of partitions
			    String [] splitInput = args[5].split("\\s+");
				conf.set("ANum", splitInput[0]); // pass the table size -Article_id
				conf.set("BNum", splitInput[1]); // pass the table size -Person_id
				System.setProperty("hadoop.home/dir", "/");
				int num_reducers = Integer.parseInt(splitInput[0]) * Integer.parseInt(splitInput[1]);
				int rounds = Integer.parseInt(args[9]);
				conf.set("r", String.valueOf(num_reducers)); // pass the num_reducers to newPartitioner Class
				long [] elaspeJobTimeArr = new long [rounds]; 
				int totalTime = 0;			
				for (int i=0; i< rounds; i++)
				{
					elaspeJobTimeArr[i] = myRunJob(conf, args, num_reducers, String.valueOf(i));	
					System.out.println("Job "+ i +" took "+ ((elaspeJobTimeArr[i] /1000) /60) + " minutes and " +((elaspeJobTimeArr[i] /1000)%60) + " seconds");	
					totalTime += elaspeJobTimeArr[i];
				}
				for (int i=0; i< rounds; i++)
					System.out.println("Job "+ i +" took "+ ((elaspeJobTimeArr[i] /1000) /60) + " minutes and " +((elaspeJobTimeArr[i] /1000)%60) + " seconds");	
				 
				System.out.println("Average Job took "+ (((totalTime /rounds)/1000) /60) + " minutes and " + (((totalTime /rounds)/1000)%60) + " seconds");		
				return(0);	  
	
    }
	
	public static long myRunJob (Configuration conf, String [] args, int num_reducers, String index) throws ClassNotFoundException, IOException, InterruptedException
	{
		Job job = Job.getInstance(conf, args[8]);
		job.setJarByClass(AcmMJSort.class);
		job.setPartitionerClass(newPartitionerClass.class); 
		job.setGroupingComparatorClass(TextPair.FirstComparator.class); 
		job.setReducerClass(IndexReduceOneLoop.class);
		//job.setReducerClass(SQLReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class); // needed?
		job.setOutputFormatClass(TextOutputFormat.class); // needed?
		job.setMapOutputKeyClass(TextPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(Integer.valueOf(args[6]));
		job.setNumReduceTasks(Integer.valueOf(num_reducers));
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, XMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, YMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, ZMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(new Path(args[3]),index));
		System.out.println("Num reducers: " + String.valueOf(num_reducers) + "\nSlaves list: " + args[6] + "\nDownlinks list: " + args[7]);		
		long start1 = new Date().getTime();
	    if (!job.waitForCompletion(true))
	    	System.exit(1);  
	    return (new Date().getTime() -start1);
	}
	public static void main(String[] args) throws Exception 
	{
		int exitcode = ToolRunner.run(new AcmMJSort(), args);
		System.exit(exitcode);
	}// main

}// AcmMJSort class
