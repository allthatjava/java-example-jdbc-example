package brian.example.java.jdbc.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	
	private static Connection con;

	public static Connection getConnection() {
		
		if( con != null)
			return con;
		
		try {
			con = DriverManager.getConnection(  
					"jdbc:postgresql://localhost:5432/postgres","postgres","admin");
			con.setAutoCommit(false);
			
		} catch (Exception e) {
			System.out.println("Connection error");
			e.printStackTrace();
		}  
		
		return con;
	}
	
	public static void close() {
		if( con != null )
			try {
				con.setAutoCommit(true);
				con.close(); 
			} catch (SQLException e) {}
	}
	
}
