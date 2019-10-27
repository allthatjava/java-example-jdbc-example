package brian.example.java.jdbc;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import brian.example.java.jdbc.connection.DBConnection;
import brian.example.java.jdbc.data.MultipleData;
import brian.example.java.jdbc.data.SingleData;

@SpringBootApplication
public class BootExampleJDBC {

	public static void main(String[] args) {
		SpringApplication.run(BootExampleJDBC.class, args);

		BootExampleJDBC app = new BootExampleJDBC();
		app.process();
	}

	public void process() {

		System.out.println( "Process started." );
		
		Instant start = Instant.now();

		Connection con = DBConnection.getConnection();

		SingleData single = new SingleData(con);
		MultipleData multiple = new MultipleData(con);
		
		// Backup previous data
//		single.backupCurrentDataTable();
		
		// Clear current data table
		single.deleteData();
		
		// Read massive data and insert multiple records at a time
		multiple.multipleInsertionExample();		
		
		Instant finish = Instant.now();
		
		System.out.println( "Elapse Time:"+ Duration.between(start, finish).toMillis()  );
	}
}
