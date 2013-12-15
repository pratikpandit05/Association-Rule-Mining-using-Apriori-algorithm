import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.BufferedReader;


public class Task1 {
	public static PrintWriter task1IO;
	
	public static void processTask1 (float sup, String username, String password) throws SQLException, IOException{
		
		
		    // Load the Oracle JDBC driver
		    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

		    // Connect to the database
		    // You must put a database name after the @ sign in the connection URL.
		    // You can use either the fully specified SQL*net syntax or a short cut
		    // syntax as <host>:<port>:<sid>.  The example uses the short cut syntax.
		    Connection conn =DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",username, password);
		    
		    task1IO = new PrintWriter(new BufferedWriter(new FileWriter("system.out.1")),true);
		    
		    // Create a Statement
		    Statement stmt = conn.createStatement ();
		    
		    //Calculate minimum support
		    float support=sup;
		    float totalTransInt=0;
		    ResultSet totaltrans = stmt.executeQuery ("SELECT COUNT(DISTINCT TRANSID) from Trans");
		    while (totaltrans.next ())
			   	  totalTransInt=Integer.parseInt(totaltrans.getString (1));
		    
		    double minsup =Math.ceil((support*totalTransInt)/100);
		    //System.out.println(minsup);
		    
	
		    //Create Frequent 1-ItemSet Table and adding tuples
		    stmt.executeQuery("CREATE TABLE Freq1 (ITEMID1 INTEGER, SUPPORT INTEGER)");
		    stmt.executeQuery("INSERT INTO Freq1 SELECT ITEMID, COUNT(*) FROM TRANS HAVING COUNT(*)>= "+minsup+" GROUP BY ITEMID");
		    
		    //Join with Items and output in file
		    String newline="\r\n";
		    ResultSet itemInfo=stmt.executeQuery("SELECT q.ITEMNAME, p.SUPPORT FROM Freq1 p, Items q WHERE p.ITEMID1=q.ITEMID");
		    while (itemInfo.next ())
		    	Task1.task1IO.println("{"+itemInfo.getString(1)+"}, s="+((Float.parseFloat(itemInfo.getString(2))*100)/totalTransInt)+"%");
		    	
		  
			   			  
		    stmt.executeQuery("DROP TABLE Freq1");
		    
		    conn.close(); // ** IMPORTANT : Close connections when done **
		  }

}