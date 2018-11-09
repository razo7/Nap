package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class testDB 
{
	private static String connectionURL1="jdbc:mysql://localhost:3306/acm";
	
	private static String user="root";
	private static String password="root";
	private static Connection connection= null;
	private static Statement statement= null;
	//public testDB()	{}
	private static String tableStudents = "students";
	private static String tableCourses = "courses";
	private static String tableLectures = "lectures";
	private static String xTable = "x_persons_103232";
	
	
	private static void connectDB()
	{
		try{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			//Class.forName("myDriver.ClassName");
			connection = DriverManager.getConnection(connectionURL1, user, password);
			statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		   }
		catch(Exception e){	}
	}


	public static void closeDB()
	{		
		try{
		statement.close();
		connection.close();
		   }
		
		catch(Exception e){	}
	}
	
	public static void main(String[] args) throws SQLException 
	{
		
		//connectDB();
		try{
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			//Class.forName("myDriver.ClassName");
			connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/acm", "root", "root");
			statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		   }
		catch(Exception e){	}
		/*
		 * statement.executeUpdate("drop database if exists AMJ");
		 
		statement.executeUpdate("create database AMJ");
		statement.executeUpdate("use AMJ");
		statement.executeUpdate("drop table if exists mmosh");
		statement.executeUpdate("create table mmosh( StudentName25	VARCHAR(20) NOT NULL,CourseName22   VARCHAR(20) NOT NULL)");
		*/
		//String sql = "SELECT * FROM "+ tableLectures;
		
		//statement.executeUpdate("CREATE TABLE res2 (select person_id,article_102.article_id,publication_id "
	//			+ "from article_102 inner join article_author_102 on article_102.article_id = article_author_102.article_id)");
		//statement.executeUpdate("CREATE TABLE res2_final (select persons_102.person_id,first_name,last_name,article_id,publication_id from persons_102 inner join res2 on persons_102.person_id = res2.person_id)");
		String s1 = "HH";
		String s2 = "dsdHH";
		String sql2 = "SELECT * FROM article_author_102";
		ResultSet rs2 = statement.executeQuery(sql2);
		//statement.executeUpdate("CREATE TABLE x_persons_102_2 (person_id VARCHAR(20), first_name VARCHAR(250), last_name VARCHAR(250) ) ");
		
		statement.executeUpdate("insert into article_author_102  values ('"+ s1 + "', '" + s2 +"')");
		String sql = "SELECT * FROM res2_final";
		ResultSet rs = statement.executeQuery(sql);
		//statement.executeUpdate("CREATE TABLE "+ xTable + " (person_id CHAR(20), first_name CHAR(250), last_name CHAR(250)) ");
				//rs.last();
	 //	int numRow = rs.getRow();
	 	//rs.beforeFirst();
	// 	String [] users = new String [numRow];
	 //	int i = 0;
		System.out.println("person_id | first_name | last_name | article_id | publication_id");
		while (rs.next())
		{
			//users[i]= rs.getString("StudentName");
			System.out.println(rs.getString("person_id") + ", " + rs.getString("first_name") + ", " + rs.getString("last_name") 
					+ ", " + rs.getString("article_id") + ", " + rs.getString("publication_id"));
		//	i++;
		}		
		closeDB();

	}//main

	
	
	
}//testDB
