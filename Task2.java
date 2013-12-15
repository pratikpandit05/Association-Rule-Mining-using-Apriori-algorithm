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


public class Task2 {
	public static PrintWriter task2IO;
	
	public static void processTask2 (float sup, String username, String password) throws SQLException, IOException{
		
			String newline="\r\n";
		    // Load the Oracle JDBC driver
		    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

		    // Connect to the database
		    Connection conn =DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",username, password);
		    
		    task2IO = new PrintWriter(new BufferedWriter(new FileWriter("system.out.2")), true);
		    
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
		    ResultSet itemInfo1=stmt.executeQuery("SELECT q.ITEMNAME, p.SUPPORT FROM Freq1 p, Items q WHERE p.ITEMID1=q.ITEMID");
		    while (itemInfo1.next ())
		    	Task2.task2IO.println("{"+itemInfo1.getString(1)+"}, s="+((Float.parseFloat(itemInfo1.getString(2))*100)/totalTransInt)+"%");
		    
		   
		    
		    //Create Candidate 2-Itemset Table and adding tuples
		    stmt.executeQuery("CREATE TABLE Cand2 (TRANSID INTEGER, ITEMID1 INTEGER, ITEMID2 INTEGER)");
		    stmt.executeQuery("INSERT INTO Cand2 SELECT p.TRANSID, p.ITEMID, q.ITEMID FROM Trans p, Trans q WHERE p.TRANSID=q.TRANSID AND q.ITEMID>p.ITEMID");
		    
		    //Create Frequent 2-Itemset Table and adding tuples
		    stmt.executeQuery("CREATE TABLE Freq2 (ITEMID1 INTEGER, ITEMID2 INTEGER, SUPPORT INTEGER)");
		    stmt.executeQuery("INSERT INTO Freq2 SELECT p.ITEMID1, p.ITEMID2, COUNT(*) FROM Cand2 p GROUP BY p.ITEMID1, p.ITEMID2 HAVING COUNT(*)>= "+minsup);
		    
		    //Join with Items and output in file
		    ResultSet itemInfo2=stmt.executeQuery("SELECT p.ITEMNAME as ITEM1, q.ITEMNAME as ITEM2, r.SUPPORT FROM ITEMS p, ITEMS q, Freq2 r WHERE r.ITEMID1=p.ITEMID AND r.ITEMID2=q.ITEMID");
		    while (itemInfo2.next ())
		    	Task2.task2IO.println("{"+itemInfo2.getString(1)+", "+itemInfo2.getString(2)+"}, s="+((Float.parseFloat(itemInfo2.getString(3))*100)/totalTransInt)+"%");
		    
	
		    
		    stmt.executeQuery("DROP TABLE Freq1");
		    stmt.executeQuery("DROP TABLE Freq2");
		    stmt.executeQuery("DROP TABLE Cand2");
		    
		    conn.close(); // ** IMPORTANT : Close connections when done **
		  }

}