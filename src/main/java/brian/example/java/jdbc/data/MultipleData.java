package brian.example.java.jdbc.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import brian.example.java.jdbc.connection.DBConnection;

public class MultipleData {
	
	Connection con;
	
	public MultipleData (Connection con) {
		this.con = con;
	}

	public void multipleInsertionExample() {
		
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File("src/main/resources/10K_records.csv"))));) {
		
			List<String> oneThousandRecords = new ArrayList<>();
			String line;
			while ((line = br.readLine()) != null) {

				oneThousandRecords.add(line);

				// Every 1000 record, we insert together
				if (oneThousandRecords.size() == 1000) {
					
					// Insert the record into database
					insertMultipleData(con, oneThousandRecords);
					oneThousandRecords.clear();
				} 
			}
			
			// Insert the leftover records
			if( oneThousandRecords.isEmpty() == false )
				insertMultipleData(con, oneThousandRecords);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			// Disconnect the connection
			DBConnection.close();
		}
	}
	
	
	/**
	 * Insert multiple data into a table
	 * @param con
	 * @param oneThousandRecord
	 * @throws SQLException
	 */
	public void insertMultipleData(Connection con, List<String> oneThousandRecord) throws SQLException {
		
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(
										"INSERT INTO JDBC_TEST(NAME1,AGE1,NAME2,AGE2,NAME3,AGE3,NAME4,AGE4,NAME5,AGE5) "
										+ " VALUES(? ,?, ?, ?, ?, ?, ?, ?, ?, ?)");

			// Since I get 1000 at a time, we will just add them all and run it.
			for( String data : oneThousandRecord ) {
				String[] dataArr = data.split(",");
				
				pstmt.setString(1, dataArr[0]);
				pstmt.setInt(2, new Integer(dataArr[1]));
				pstmt.setString(3, dataArr[2]);
				pstmt.setInt(4, new Integer(dataArr[3]));
				pstmt.setString(5, dataArr[4]);
				pstmt.setInt(6, new Integer(dataArr[5]));
				pstmt.setString(7, dataArr[6]);
				pstmt.setInt(8, new Integer(dataArr[7]));
				pstmt.setString(9, dataArr[8]);
				pstmt.setInt(10, new Integer(dataArr[9]));
				pstmt.addBatch();
			}
			
			pstmt.executeBatch();
			con.commit();
			
			System.out.println("Inserted "+oneThousandRecord.size()+" records.");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if( pstmt != null )
				pstmt.close();
		}
		
	}
}
