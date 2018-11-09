import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

//import org.apache.hadoop.io.Text;

public class testDB2 
{
	public static Connection	connection = null;
	public static Statement statement = null;
	static String  groupKey = "104";
	static String realvalX = "2402538	2402536,2402539	2402536,2402540	2402536,2402541	2402536,2402542	2402536,2402543	2402536,2402544	2402536,2402545	2402536,2402546	2402536,2402547	2402536,2402548	2402536,2402549	2402536,2402550	2402536,2402552	2402536,2402553	2402536,2402554	2402536,2402555	2402536,2402556	2402536,2402557	2402536,2402558	2402536,2402559	2402536,2402560	2402536,2402561	2402536,2402562	2402536,2402563	2402536,2402565	2402536,2402566	2402536,2402567	2402536,2402568	2402536,2402569	2402536,2402571	2402536,2402572	2402536,2402573	2402536,2402574	2402536,2402575	2402536,2402576	2402536,2402577	2402536,2402579	2402536,2402580	2402536,2402581	2402536,2402582	2402536,2402583	2402536,2402584	2402536,2402585	2402536,2402586	2402536,2402587	2402536,805252	800166,805253	800166,805254	800166,805255	800166,805256	800166,805257	800166,805258	800166,805259	800166,805260	800166,805261	800166,805262	800166,805263	800166,805264	800166,805265	800166,805266	800166,805267	800166,805268	800166,805269	800166,805270	800166,805271	800166,805272	800166,805273	800166,805274	800166,805275	800166,805276	800166,805277	800166,805278	800166,805279	800166,805280	800166,805281	800166,805282	800166,805283	800166,805284	800166,805285	800166,805286	800166,805287	800166,805288	800166,805289	800166,805290	800166,805291	800166,805292	800166,805293	800166,805294	800166,805295	800166,805296	800166,805297	800166,805298	800166,805299	800166,805300	800166,805301	800166,805302	800166,805303	800166,805304	800166,805305	800166";
	static String realvalY = "2402538	P3904848,2402539	P3904849,2402540	P3904850,2402540	P3904851,2402541	P3904852,2402541	P3904853,2402541	P3904854,2402542	P3904855,2402542	P3904856,2402542	P3904857,2402543	P3904858,2402543	P3904859,2402544	P3904860,2402545	P3904861,2402545	P3904862,2402545	P3904863,2402546	P3904864,2402547	P3904865,2402548	P3904866,2402549	P3904867,2402550	P3904868,2402552	P3904869,2402552	P3904870,2402552	P3904871,2402552	P3904872,2402553	P3904873,2402553	P3904874,2402554	P3904875,2402554	P3904876,2402554	P3904877,2402555	P3904878,2402555	P3904879,2402555	P3904880,2402556	P3904881,2402556	P3904882,2402556	P3904883,2402557	P3904884,2402557	P3904885,2402558	P3904886,2402558	P3904887,2402558	P3904888,2402559	P3904889,2402560	P3904890,2402561	P3904891,2402562	P3904892,2402562	P3904893,2402563	P3904894,2402565	P3904895,2402566	P3904896,2402567	P3904897,2402568	P3904898,2402569	P3904899,2402571	P3904900,2402572	P3904901,2402573	P3904902,2402573	P3904903,2402574	P3904904,2402574	P3904905,2402574	P3904906,2402575	P3904907,2402575	P3904908,2402576	P3904909,2402577	P3904910,2402579	P3904911,2402580	P3904912,2402580	P3904913,2402581	P3904914,2402581	P3904915,2402582	P3904916,2402582	P3904917,2402583	P3904918,2402584	P3904919,2402585	P3904920,2402586	P3904921,2402587	P3904922,805252	P331567,805253	P95456,805254	PP14131701,805255	P116968,805256	P332443,805257	P330716,805258	PP39053136,805258	P329143,805258	PP39040761,805259	P226321,805260	PP14172198,805260	PP14024357,805261	P334284,805261	P330010,805261	P329670,805262	P328791,805263	P333128,805264	P328770,805264	P329297,805264	P333614,805264	P330846,805264	P334305,805265	P328823,805265	P329650,805265	P329651";
	static String realvalZ = "P116968	Irwin	Kabak,P21812	Arnold	Ockene,P226321	Philip	Kiviat,P328770	A.	Beged Dov,P328791	A.	Economos,P329143	BJ\\_amp\\_#248;rn	Myhrhaug,P329297	C.	Carmichael,P329670	D.	Townsend,P330010	E.	Katz,P330716	Harry	Felder,P331356	Jeffrey	Waxweiler,P331567	John	Snyder,P332443	Michael	Gilman,P333128	R.	Soule,P334284	W.	Neisius,P3904846	Melvin	Klerer,P3904847	Juris	Reiiifelds,P3904848	Melvin	Klerer,P3904849	Burton	Fried,P3904850	A.	Falkoff,P3904851	K.	Iverson,P3904852	Roger	Lazarus,P3904853	Mark	Wells,P3904854	John	Wooten,P3904855	Robert	Seitz,P3904856	Lawrence	Wood,P3904857	Charles	Ely,P3904858	Lawrence	Symes,P3904859	Roger	Roman,P3904860	Anthony	Hearn,P3904861	Stewart	Schlesinger,P3904862	Lawrence	Sashkin,P3904863	Kenneth	Reed,P3904864	Howard	Matthews,P3904865	Gerald	Bradley,P3904866	Rene	De Vogelaere,P3904867	Edgar	Sibley,P3904868	Jonathan	Millen,P3904869	Raymond	Wiesen,P3904870	Douwe	Yntema,P3904871	James	Forgie,P3904872	Arthur	Stowe,P3904873	Krzysztof	Frankowski,P3904874	C.	Zimmerman,P3904875	S.	Schlesinger,P3904876	L.	Sashkin,P3904877	C.	Aumann,P3904878	Melvin	Klerer,P3904879	Fred	Grossman,P3904880	Charles	Amann,P3904881	D.	Manelski,P3904882	H.	Lefkovits,P3904883	H.	Hebert,P3904884	C.	Abraham,P3904885	T.	Pearcey,P3904886	J.	Mitchell,P3904887	A.	Perlis,P3904888	H.	Van Zoeren,P3904889	A.	Jett,P3904890	Richard	Plocica,P3904891	Kenneth	Lock,P3904892	Kenneth	Busch,P3904893	Gottfried	Luderer,P3904894	Adrian	Ruyle,P3904895	E.	Coffman,P3904896	G.	Patton,P3904897	James	Haag,P3904898	G.	Manacher,P3904899	Richard	Cullen,P3904900	John	Rice,P3904901	Herbert	Bright,P3904902	L.	Gallaher,P3904903	I.	Perlin,P3904904	Joseph	Muskat,P3904905	Francis	Sullivan,P3904906	Paul	Borman,P3904907	Arthur	Priver,P3904908	Barry	Boehm,P3904909	Lyle	Smith,P3904910	Anne	Ammerman,P3904911	Glen	Culler,P3904912	Peter	Hill,P3904913	Arthur	Stowe,P3904914	L.	Breed,P3904915	R.	Lathwell,P3904916	Roger	Roman,P3904917	Lawrence	Symes,P3904918	Juris	Reinfelds,P3904919	Carlos	Christensen,P3904920	Robert	Anderson,P3904921	Helen	Willett,P3904922	Burton	Fried,P95456	George	Fishman,PP14024357	Benjamin	Mittman,PP14131701	A.	Frank,PP14135248	Howard	Falk,PP14172198	Martin	Goldberg,PP39040761	Kristen	Nygaard,PP39053136	Ole-Johan	Dahl,PP39109729	Julian	Reitman";
	public static void main(String[] args) throws SQLException 
	{
		try
		{
		System.out.println("start");
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		System.out.println("Class-start");
		Connection	connection = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
		System.out.println("connection");
		Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		System.out.println("oneByone-start");
		oneByone(statement);
		System.out.println("oneByone-end");
		System.out.println("allInone-start");
		allInone(statement);
		System.out.println("allInone-end");
		
		/*System.out.println("statement");
		String sql = "SELECT * FROM article_102 limit 4";
		ResultSet rs = statement.executeQuery(sql);
		System.out.println("article_id | publication_id");
		while (rs.next())
			System.out.println(rs.getString("article_id") + ", " + rs.getString("publication_id"));
			*/
		statement.close();
		connection.close();
		}//try
				catch(Exception e){	System. out.println(e.toString());}
		System.out.println("End main");
		}//main
	
	public static void oneByone (Statement statement)
	{
		try
		{
		String [] ValX = realvalX.toString().split(",");
		String [] ValY = realvalY.toString().split(",");
		String [] ValZ = realvalZ.toString().split(",");
		long start6 = 0;
		long start1 = new Date().getTime();
		 String xTable = "x_article_" + groupKey;
		 String yTable = "y_article_author_" + groupKey;
		 String zTable = "z_persons_" + groupKey;
		 String x_yRes = "x_yRes" + groupKey;
         String final_res = "final_res_" + groupKey;
		 statement.executeUpdate("drop table if exists "+ xTable + "," + yTable + "," + zTable + "," + x_yRes + "," + final_res );//clear old tables
		  statement.executeUpdate("CREATE TABLE "+ xTable + " (article_id CHAR(20), publication_id CHAR(20)) ");
	      statement.executeUpdate("CREATE TABLE "+ yTable + " (article_id CHAR(20), person_id CHAR(20)) ");
         statement.executeUpdate("CREATE TABLE "+ zTable + " (person_id CHAR(20), first_name CHAR(250), last_name CHAR(250)) ");
         long start2 = new Date().getTime();
         for (int i =0; i< ValX.length; i++)
         {
        	 String [] Val = ValX[i].toString().split("\t");
        	 statement.executeUpdate("insert into "+ xTable +" values ('" + Val[0] + "','" + Val[1] + "')");//article
         }
         for (int i =0; i< ValY.length; i++)
         {
        	 String [] Val = ValY[i].toString().split("\t");
        	 statement.executeUpdate("insert into "+ yTable +" values ('" + Val[0] + "','" + Val[1] + "')");//article
         }
         for (int i =0; i< ValZ.length; i++)
         {
        	 String [] Val = ValZ[i].toString().split("\t");
        	 statement.executeUpdate("insert into "+ zTable +" values ('" + Val[0] + "','" + Val[1] + "','" + Val[2] + "')");//article
         }
         
         long start3 = new Date().getTime();
         //System.out.println("start join");
		  int res1 =  statement.executeUpdate("CREATE TABLE " + x_yRes + " (select person_id, "+ xTable + ".article_id, publication_id "
		      + "from "+ xTable + " inner join "+ yTable + " on " + xTable + ".article_id = " + yTable + ".article_id)");
		  long start4 = new Date().getTime();
		  System.out.println(res1 + " firstJoinRows");
		  long start5 = new Date().getTime();
		  if (res1 > 0)
		  {
			  int res2 = statement.executeUpdate("CREATE TABLE " + final_res + " (select "+ zTable + ".person_id, first_name, last_name, article_id, publication_id "
			  + "from "+ zTable + " inner join " + x_yRes + " on " + zTable + ".person_id = " + x_yRes + ".person_id)");
			   start6 = new Date().getTime();
			  System.out.println("start to write, " + res2 + " secondJoinRows");
		      if (res2 > 0)
		      {
		    	  ResultSet rs = statement.executeQuery("SELECT * FROM " + final_res);
		    	  while (rs.next())
		    		System.out.println( rs.getString("person_id")+ "\t" + rs.getString("first_name") +
		    				  "\t" + rs.getString("last_name") + "\t" + rs.getString("article_id") + "\t" + rs.getString("publication_id") );
		      }//if-res2
		 }//if-res1
		  
		  System.out.println("Creating table took "+ (((start2 -start1) /1000) /60) + " minutes and " +(( (start2 -start1) /1000)%60) + " seconds");
		  System.out.println("loading strings &inserting tables took "+ (((start3 -start2) /1000) /60) + " minutes and " +(( (start3 -start2) /1000)%60) + " seconds");
		  System.out.println("Join 1 took "+ (((start4 -start3) /1000) /60) + " minutes and " +(( (start4 -start3) /1000)%60) + " seconds");
		  System.out.println("Join 2 took "+ (((start6 -start5) /1000) /60) + " minutes and " +(( (start6 -start5) /1000)%60) + " seconds");
		  		  
		  statement.executeUpdate("drop table if exists "+ xTable + "," + yTable + "," + zTable + "," + x_yRes + "," + final_res );//clear old tables
		}
		catch(Exception e){	System. out.println(e.toString());}
		
	}
	
	public static void allInone (Statement statement)
	{
		try
		{
		String [] ValX = realvalX.toString().split(",");
		String [] ValY = realvalY.toString().split(",");
		String [] ValZ = realvalZ.toString().split(",");
		long start7 = 0;
		long start1 = new Date().getTime();
		 String xTable = "x_article_" + groupKey;
		 String yTable = "y_article_author_" + groupKey;
		 String zTable = "z_persons_" + groupKey;
		 String x_yRes = "x_yRes" + groupKey;
         String final_res = "final_res_" + groupKey;
         // statement.executeUpdate("drop table if exists "+ xTable + "," + yTable + "," + zTable + "," + x_yRes + "," + final_res );//clear old tables
		  statement.executeUpdate("CREATE TABLE "+ xTable + " (article_id CHAR(20), publication_id CHAR(20)) ");
	      statement.executeUpdate("CREATE TABLE "+ yTable + " (article_id CHAR(20), person_id CHAR(20)) ");
         statement.executeUpdate("CREATE TABLE "+ zTable + " (person_id CHAR(20), first_name CHAR(250), last_name CHAR(250)) ");
         String xres = "insert into "+ xTable +" values ";
         String yres = "insert into "+ yTable +" values ";
         String zres = "insert into "+ zTable +" values ";
         long start2 = new Date().getTime();
         for (int i =0; i< ValX.length; i++)
         {
        	 String [] Val = ValX[i].toString().split("\t");
        	 xres += "('" + Val[0] + "','" + Val[1] + "'),";
         }
         for (int i =0; i< ValY.length; i++)
         {
        	 String [] Val = ValY[i].toString().split("\t");
        	 yres += "('" + Val[0] + "','" + Val[1] + "'),";
         }
         for (int i =0; i< ValZ.length; i++)
         {
        	 String [] Val = ValZ[i].toString().split("\t");
        	 zres += "('" + Val[0] + "','" + Val[1] + "','" + Val[2] + "'),";
         }
         long start3 = new Date().getTime();
         xres = xres.substring(0, xres.length() -1);
         yres = yres.substring(0, yres.length() -1);
         zres = zres.substring(0, zres.length() -1);
         statement.executeUpdate(xres);//article
         statement.executeUpdate(yres);//article
         statement.executeUpdate(zres);//article
         
         //System.out.println("start join");
         long start4 = new Date().getTime();
		  int res1 =  statement.executeUpdate("CREATE TABLE " + x_yRes + " (select person_id, "+ xTable + ".article_id, publication_id "
		      + "from "+ xTable + " inner join "+ yTable + " on " + xTable + ".article_id = " + yTable + ".article_id)");
		  long start5 = new Date().getTime();
		  System.out.println(res1 + " firstJoinRows");
		  long start6= new Date().getTime();
		  if (res1 > 0)
		  {
			  int res2 = statement.executeUpdate("CREATE TABLE " + final_res + " (select "+ zTable + ".person_id, first_name, last_name, article_id, publication_id "
			  + "from "+ zTable + " inner join " + x_yRes + " on " + zTable + ".person_id = " + x_yRes + ".person_id)");
			  start7 = new Date().getTime();
			  System.out.println("start to write, " + res2 + " secondJoinRows");
		      if (res2 > 0)
		      {
		    	  ResultSet rs = statement.executeQuery("SELECT * FROM " + final_res);
		    	  while (rs.next())
		    		System.out.println( rs.getString("person_id")+ "\t" + rs.getString("first_name") +
		    				  "\t" + rs.getString("last_name") + "\t" + rs.getString("article_id") + "\t" + rs.getString("publication_id") );
		      }//if-res2
		 }//if-res1
		  
		  System.out.println("Creating table took "+ (((start2 -start1) /1000) /60) + " minutes and " +(( (start2 -start1) /1000)%60) + " seconds");
		  System.out.println("loading strings took "+ (((start3 -start2) /1000) /60) + " minutes and " +(( (start3 -start2) /1000)%60) + " seconds");
		  System.out.println("inserting tables took "+ (((start4 -start3) /1000) /60) + " minutes and " +(( (start4 -start3) /1000)%60) + " seconds");
		  System.out.println("Join 1 took "+ (((start5 -start4) /1000) /60) + " minutes and " +(( (start5 -start4) /1000)%60) + " seconds");
		  System.out.println("Join 2 took "+ (((start7 -start6) /1000) /60) + " minutes and " +(( (start7 -start6) /1000)%60) + " seconds");
		  
		  
		  statement.executeUpdate("drop table if exists "+ xTable + "," + yTable + "," + zTable + "," + x_yRes + "," + final_res );//clear old tables
		  
		}
		catch(Exception e){	System. out.println(e.toString());}
		
	}
}
