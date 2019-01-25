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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import run.WordCountOR2.newPartitionerClass;
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
			  
			         LOG.info("OR_Change-newPartitionerClass- Yes upload\nMappers: "+ mapperrArrayHDFS + "\nReducers: " + reducerArrayHDFS + "\nPartitionSize- " + bwNodeString + "\nslaveNames- " + NodeString);
			         String [] testNumSlavesBW = bwNodeString.split("\\s+"); // array of slave's downlinks per slave
			         if (testNumSlavesBW.length > slaveNames.length)
			         {
			        	 String [] mappersSlaves = mapperrArrayHDFS.split("\\s+"); // array of mapper's slaves according to the their ID
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
			    		 LOG.info("OR_Change-newPartitionerClass-mappers\ninfoMappersAmount: "+ infoMappersAmount + "\ninfoMappersBW: " + infoMappersBW);
			         }
			         else
			         {//average downlink
			        	 for (i=0; i< slaveNames.length; i++)
			 	        	{
			        		  selectedBW[i] = Integer.parseInt(testNumSlavesBW[i]);
			 	        	} 
			         }
			         
			         String [] reducerSlaves = reducerArrayHDFS.split("\\s+"); // array of reducer's slaves according to the their ID
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
	    public int getPartition(TextPair key, Text value, int numPartitions)
	    {	
	     int res = 0; //the default partition
	  	 if (W == 0)
	  		res = (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions; // when W=0 we partition the tuples evenly
	  	 else
	  	 {//when we have the new allocation
	  		 int oldres = (key.getFirst().hashCode() & Integer.MAX_VALUE) % W; // when W>0 we partition the tuples according to slaveSize
	  		 int slaveIndex = 0; // index of slave
	     	 int partitionIndicator = slaveSize[slaveIndex];
	       	 while (partitionIndicator == 0 || oldres >= partitionIndicator)// if PartitionSize[optPartit] is zero
	       	   { // we skip because we should try to avoid use him
	       		  slaveIndex++;
	     		  partitionIndicator += slaveSize[slaveIndex];
	     	   }//while
	       	 int realSlaveIndex = reducersSlaveIndices[slaveIndex];
	      	// LOG.info("my key - " + key + ", oldres= " +  String.valueOf(oldres) + ", slaveIndex= " + String.valueOf(slaveIndex) + ", partitionIndicator= " + String.valueOf(partitionIndicator) + ", slave= " + String.valueOf(realSlaveIndex) );
	         int toReducerIndex = (key.getFirst().hashCode() & Integer.MAX_VALUE) % counterReducers[realSlaveIndex];
	       	 res = reducerIndicesPerSlave[realSlaveIndex][toReducerIndex];
	     //  LOG.info("Ultimate Test- key = " + key + ", oldres = " + oldres + ", slaveIndex = " + slaveIndex +  ", countReducerBySlave[slaveIndex] = " + countReducerBySlave[slaveIndex] + ", toReducerIndex = " + toReducerIndex + ", res = " + res );
	     }//else
	  	return res;
	   }//fun getPartition
	}//class newPartitionerClass

	 public String getSplitSize (Configuration conf, String mappersNum, String file) throws IOException
	  {
		 String [] files = file.split("\\s+");
		 int size = 0;
		 for ( int i = 0; i < files.length; i++)
		 {
			 Path path = new Path("/user/hadoop2/" + files[i]);
			 FileSystem hdfs = path.getFileSystem(conf);
			 ContentSummary cSummary = hdfs.getContentSummary(path);
			 size += (int) cSummary.getLength();
		 }
		 System.out.println("file size = " + String.valueOf(size) ); 
		 //inputsplitSize
		 return String.valueOf(size /Integer.parseInt(mappersNum) );
	  }
	@Override
    public int run (String[] args) throws Exception
    {// input_1 input_2 input_3 output num_mappers S_Vector slave_names downlinkVec JobName rounds
				Configuration conf = getConf();
				String inputsplitSize = getSplitSize(conf, args[4], args[0]+ " " + args[1] + " " +  args[2]);
				// # of mappers = size_input / split size [Bytes], split size=  max(mapreduce.input.fileinputformat.split.minsize, min(mapreduce.input.fileinputformat.split.maxsize, dfs.blocksize))
				conf.set("mapreduce.input.fileinputformat.split.minsize", inputsplitSize); 
				conf.set("mapreduce.input.fileinputformat.split.maxsize", inputsplitSize);
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
