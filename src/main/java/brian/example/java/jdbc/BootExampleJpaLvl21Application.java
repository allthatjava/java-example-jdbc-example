package brian.example.java.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import brian.example.java.jdbc.connection.DBConnection;

@SpringBootApplication
public class BootExampleJpaLvl21Application {

	public static void main(String[] args) {
		SpringApplication.run(BootExampleJpaLvl21Application.class, args);
		
		BootExampleJpaLvl21Application app = new BootExampleJpaLvl21Application();
		app.proceed();
	}

	public void proceed() {
		
		Instant start = Instant.now();

		Connection con = DBConnection.getConnection();

		// Backup previous data
		backupCurrentDataTable(con);
		
		// Clear current data table
		deleteDataFromCurrentDataTable(con);
		
		// Read the file
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File("src/main/resources/10K_records.csv"))));) {
		
			List<String> oneThousandRecords = new ArrayList<>();
			String line;
			while ((line = br.readLine()) != null) {

				oneThousandRecords.add(line);

				// Every 1000 record, we insert together
				if (oneThousandRecords.size() == 1000) {
					
					// Insert the record into database
					insertData(con, oneThousandRecords);
					oneThousandRecords.clear();
				} 
			}
			
			// Insert the leftover records
			if( oneThousandRecords.isEmpty() == false )
				insertData(con, oneThousandRecords);
				

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
		
		Instant finish = Instant.now();
		
		System.out.println( "Elapse Time:"+ Duration.between(start, finish).toMillis()  );
		
	}

	/**
	 * Delete the current table, so we can add full fresh data
	 * @param con
	 */
	private void deleteDataFromCurrentDataTable(Connection con) {
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement("DELETE FROM TEST");
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
	private void backupCurrentDataTable(Connection con) {
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

	private String padLeft(String src, String padStr, int totalLength)
	{
		if(src.length() == totalLength)
			return src;
		
		StringBuilder sb = new StringBuilder(padStr);
		while( src.length() != totalLength) {
			src = sb.append(src).toString();
		}
		
		return sb.toString();
	}
	
	/**
	 * Insert multiple data into a table
	 * @param con
	 * @param oneThousandRecord
	 * @throws SQLException
	 */
	private void insertData(Connection con, List<String> oneThousandRecord) throws SQLException {
		
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(
										"INSERT INTO TEST(NAME1,AGE1,NAME2,AGE2,NAME3,AGE3,NAME4,AGE4,NAME5,AGE5) "
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
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if( pstmt != null )
				pstmt.close();
		}
		
	}
}
