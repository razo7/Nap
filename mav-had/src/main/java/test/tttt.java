package test;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
public class tttt {
private final static Random rand = new Random();
	public static void main(String[] args){
		// TODO Auto-generated method stub
		String a = "0";
		String b = "1";
		String res = a+b;
		//System.out.println(("0"+"0").hashCode() + " " + ("0"+"1").hashCode() + " " + ("1"+"0").hashCode() + " " + ("1"+"1").hashCode() );
		int A=5;
		int B=5;
		for (int i=0; i< A; i++)
		{
			String a1 = String.valueOf(i);
			for (int j=0; j< B; j++)
			{
				String b1 = String.valueOf(j);
                String res3 =a1+b1;
				System.out.println(res3 +": "+ res3.hashCode()  );
			}}
		System.out.println(closeNum(19));
		int r = 5;
		int W = 7;
		int x = Math.round(2*(float)W/r);
		System.out.println(x);
		int [][] indexReducerBySlave = new int [3][8];
	     int [] countReducerBySlave = {0,0,0};
	     ++countReducerBySlave[0];
	    
	     indexReducerBySlave[0][0] = 2;
	 	System.out.println("a " +": "+ "a ".hashCode()  );
		
     }
	public static int closeNum (int num)
	{
		int cand = (int) Math.floor(Math.sqrt(num));
		while (cand > 0)
		{
			if (num % cand == 0)
				break;
			cand--;
		}
		return cand;
		
	}
}
