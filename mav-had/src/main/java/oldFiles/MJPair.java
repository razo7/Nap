package oldFiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class MJPair implements WritableComparable<MJPair> 
{
	private Text reducerIndex; 
	private Text opString;

	public MJPair() 
	{ 
		set(new Text(), new Text());
	}
	public MJPair(String reducerIndex, String opString) 
	{ 
		set(new Text(reducerIndex), new Text(opString));
	}
	public MJPair(Text reducerIndex, Text opString) 
	{ 
		set(reducerIndex, opString);
	} 
	public void set(Text reducerIndex, Text opString) 
	{
		this.reducerIndex = reducerIndex; 
		this.opString = opString;
	}
	public Text getreducerIndex() 
	{ 
		return reducerIndex;
	}
	public Text getopString() 
	{ 
		return opString;
	}
	@Override 
	public void write(DataOutput out) throws IOException 
	{ 
		reducerIndex.write(out); 
		opString.write(out);
	}
	@Override 
	public void readFields(DataInput in) throws IOException 
	{
		reducerIndex.readFields(in); 
		opString.readFields(in);
	}
	@Override 
	public int hashCode() 
	{ 
		return reducerIndex.hashCode() * 163 + opString.hashCode();
	}
	@Override 
	public boolean equals(Object o) 
	{ 	
		if (o instanceof MJPair) 
		{ 
			MJPair tp = (MJPair) o; 
			return reducerIndex.equals(tp.reducerIndex) && opString.equals(tp.opString);
		} 
		return false; 
	}

	@Override 
	public String toString() 
	{ 
		return reducerIndex + "\t" + opString;	
	}

	@Override 
	public int compareTo(MJPair mjp) 
	{ 
		int cmp = reducerIndex.compareTo(mjp.reducerIndex); 
		if (cmp != 0) 
		{ 
			return cmp;
		} 
		return opString.compareTo(mjp.opString);
	}
	
	public static class Comparator extends WritableComparator 
	{ 
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		public Comparator() 
		{ 
			super(MJPair.class);
		}
	
		@Override 
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
		{
	
			try 
			{ 
				int reducerIndexL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1); 
				int reducerIndexL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2); 
				int cmp = TEXT_COMPARATOR.compare(b1, s1, reducerIndexL1, b2, s2, reducerIndexL2); 
				if (cmp != 0) 
				{ 
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + reducerIndexL1, l1 - reducerIndexL1, b2, s2 + reducerIndexL2, l2 - reducerIndexL2);
			} 
			catch (IOException e) 
			{ 
				throw new IllegalArgumentException(e);
			}			
		}//compare
/*
		@SuppressWarnings("rawtypes")
		@Override
		 public int compare(WritableComparable a, WritableComparable b) 
		 {
			MJPair k1 = (MJPair) a;
			MJPair k2 = (MJPair) b;
			int result = k1.getreducerIndex().compareTo(k2.getreducerIndex());
			if (result == 0)
			{
				String op1 = k1.getopString().toString();
				String finalVal = op1.substring(1, op1.length());
				String [] Val1 = finalVal.split(",");
				char table1 = op1.charAt(0);
				String op2 = k2.getopString().toString();
				String finalVal2 = op2.substring(1, op2.length());
				String [] Val2 = finalVal2.split(",");
				char table2 = op2.charAt(0);
				if (table1 == table2)
				{
					switch (table1)
					{
					    case 'X':
					     {
					    	 result = Val1[0].compareTo(Val2[0]);
					    	 if (result == 0)
								{
					    		 result = Val1[1].compareTo(Val2[1]);
								}
					    	 break;
					     }//article
					    case 'Y':
					     {
					    	 result = Val1[1].compareTo(Val2[1]);
					    	 if (result == 0)
								{
					    		 result = Val1[0].compareTo(Val2[0]);
								}
					    	 break;
					     }//article_author
					     default:
					     {
					    	 result = Val1[0].compareTo(Val2[0]);
					    	 if (result == 0)
								{
					    		 result = Val1[1].compareTo(Val2[1]);
					    		 if (result == 0)
									{
						    		 result = Val1[2].compareTo(Val2[2]);
									}
								}
					    	 break;
					     }//persons
					}//switch	
					
				}//if-same table
				if (table1 == 'X')
					result = 3;
				else if (table1 == 'Y')
					result = 2;
				else
					result = 1;
			}//if- same reducer index
			    return result;
		 }//compare 
		 
*/
	}//class Comparator 
	
	static 
	{ 
		WritableComparator.define(MJPair.class, new Comparator());
	}

	public static class MJPairGrouper extends WritableComparator 
	{
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		public MJPairGrouper() 
		{ 
			super(MJPair.class);
		}
		@Override 
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
		{
			try 
			{ 
				int reducerIndexL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1); 
				int reducerIndexL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2); 
				return TEXT_COMPARATOR.compare(b1, s1, reducerIndexL1, b2, s2, reducerIndexL2);
				} 
			catch (IOException e) 
			{
				throw new IllegalArgumentException(e);
			} 
		}
		@SuppressWarnings("rawtypes")
		@Override 
		public int compare(WritableComparable a, WritableComparable b) 
		{ 
			if (a instanceof MJPair && b instanceof MJPair) 
			{ 
				return ((MJPair) a).reducerIndex.compareTo(((MJPair) b).reducerIndex);
			} 
			return super.compare(a, b); 
		} //compare
	}//class MJPairGrouper 


}//class MJPair