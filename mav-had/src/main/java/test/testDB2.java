package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class testDB2 
{
	public static void main(String[] args) throws SQLException 
	{
		try
		{
		System.out.println("start");
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		System.out.println("Class-start");
		Connection	connection = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/acm", "root", "root");
		System.out.println("connection");
		Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		System.out.println("statement");
		String sql = "SELECT * FROM article_102 limit 4";
		ResultSet rs = statement.executeQuery(sql);
		System.out.println("article_id | publication_id");
		while (rs.next())
			System.out.println(rs.getString("article_id") + ", " + rs.getString("publication_id"));
		statement.close();
		connection.close();
		}//try
				catch(Exception e){	System. out.println(e.toString());}
		System.out.println("End main");
		}//main
	
}