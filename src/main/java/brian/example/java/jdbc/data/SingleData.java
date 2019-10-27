package brian.example.java.jdbc.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;

public class SingleData {
	
	private Connection con = null;
	
	public SingleData(Connection con) {
		this.con = con;
	}

	/**
	 * Delete the current table, so we can add full fresh data
	 * @param con
	 */
	public void deleteData() {
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement("DELETE FROM JDBC_TEST");
			pstmt.execute();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			if(pstmt != null)
				try{pstmt.close();}catch(Exception e) {};
		}
	}

	/**
	 * Create a backup data table with current date
	 * @param con
	 */
	public void backupCurrentDataTable() {
		PreparedStatement pstmt = null;
		try {
			LocalDateTime dt = LocalDateTime.now();
			
			pstmt = con.prepareStatement(
					"SELECT * INTO TEST_"+dt.getYear()
						+padLeft(dt.getMonthValue()+"", "0", 2)
						+padLeft(dt.getDayOfMonth()+"", "0", 2)
						+" FROM TEST");
			pstmt.execute();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			if(pstmt != null)
				try{pstmt.close();}catch(Exception e) {};
		}
	}

	private static String padLeft(String src, String padStr, int totalLength)
	{
		if(src.length() == totalLength)
			return src;
		
		StringBuilder sb = new StringBuilder(padStr);
		while( src.length() != totalLength) {
			src = sb.append(src).toString();
		}
		
		return sb.toString();
	}
}
