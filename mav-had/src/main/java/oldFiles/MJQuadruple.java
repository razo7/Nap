package oldFiles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class MJQuadruple implements WritableComparable<MJQuadruple> 
{
	private Text reducerIndex; 
	private Text tableName;
	private Text firstAttribute;
	private Text secondAttribute;
	

	public MJQuadruple() 
	{ 
		set(new Text(), new Text(), new Text(), new Text());
	}
	public MJQuadruple(String reducerIndex, String tableName, String firstAttribute, String secondAttribute)
	{ 
		set(new Text(reducerIndex), new Text(tableName), new Text(firstAttribute), new Text(secondAttribute));
	}
	public MJQuadruple(Text reducerIndex, Text tableName, Text firstAttribute, Text secondAttribute)
	{ 
		set(reducerIndex, tableName, firstAttribute, secondAttribute);
	} 
	public void set(Text reducerIndex, Text tableName, Text firstAttribute, Text secondAttribute)
	{
		this.reducerIndex = reducerIndex; 
		this.tableName = tableName;
		this.firstAttribute = firstAttribute;
		this.secondAttribute = secondAttribute;
	}
	public Text getreducerIndex() 
	{ 
		return reducerIndex;
	}
	public Text getTableName() { return  tableName;}
	public Text getFirstAttribute() { return  firstAttribute;}
	public Text getSecondAttribute() { return secondAttribute;}

	@Override
	public void write(DataOutput out) throws IOException 
	{ 
		reducerIndex.write(out); 
		tableName.write(out);
		firstAttribute.write(out);
		secondAttribute.write(out);
	}
	@Override 
	public void readFields(DataInput in) throws IOException 
	{
		reducerIndex.readFields(in); 
		tableName.readFields(in);
		firstAttribute.readFields(in);
		secondAttribute.readFields(in);}
	@Override 
	public int hashCode() 
	{ 
		return reducerIndex.hashCode() * 163 + tableName.hashCode() * 80 +
				firstAttribute.hashCode() * 20 + secondAttribute.hashCode() ;
	}
	@Override 
	public boolean equals(Object o) 
	{ 	
		if (o instanceof MJQuadruple) 
		{ 
			MJQuadruple tp = (MJQuadruple) o; 
			return reducerIndex.equals(tp.reducerIndex) && tableName.equals(tp.tableName) &&
					firstAttribute.equals(tp.firstAttribute) && secondAttribute.equals(tp.secondAttribute) ;
		} 
		return false; 
	}

	@Override 
	public String toString() 
	{ 
		return reducerIndex + "\t" + tableName + "\t"
				+ firstAttribute + "\t" + secondAttribute;
	}

	@Override 
	public int compareTo(MJQuadruple mjp) 
	{ 
		int cmp = reducerIndex.compareTo(mjp.reducerIndex); 
		if (cmp != 0) { return cmp;	}
		cmp = tableName.compareTo(mjp.tableName);
		if (cmp != 0) { return cmp;	}
		cmp = firstAttribute.compareTo(mjp.firstAttribute);
		if (cmp != 0) { return cmp;	}
		return secondAttribute.compareTo(mjp.secondAttribute);

	}
	
	public static class Comparator extends WritableComparator 
	{ 
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		public Comparator() 
		{ 
			super(MJQuadruple.class);
		}
	
		@Override 
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
		{
	
			try 
			{ 
				int A1One = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int A1Two = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, A1One, b2, s2, A1Two);
				if (cmp != 0) { return cmp;}
				int A2One = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, A1One+s1);
				int A2Two = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, A1Two+s2);
				cmp = TEXT_COMPARATOR.compare(b1, s1, A2One, b2, s2, A2Two);
				if (cmp != 0) { return cmp;}
				int A3One = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1+A2One);
				int A3Two = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2+A2Two);
				cmp = TEXT_COMPARATOR.compare(b1, s1, A3One, b2, s2, A3Two);
				if (cmp != 0) { return cmp;}
				return TEXT_COMPARATOR.compare(b1, s1 + A3One, l1 - A3One, b2, s2 + A3Two, l2 - A3Two);
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
		WritableComparator.define(MJQuadruple.class, new Comparator());
	}

	public static class MJQuadrupleGrouper extends WritableComparator 
	{
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		public MJQuadrupleGrouper() 
		{ 
			super(MJQuadruple.class);
		}
		@Override 
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
		{
			try 
			{ 
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
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
			if (a instanceof MJQuadruple && b instanceof MJQuadruple) 
			{ 
				return ((MJQuadruple) a).reducerIndex.compareTo(((MJQuadruple) b).reducerIndex);
			} 
			return super.compare(a, b); 
		} //compare
	}//class MJPairGrouper 


}//class MJPair