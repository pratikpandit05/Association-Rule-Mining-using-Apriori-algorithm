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


public class Task3 {
	public static PrintWriter task3IO;
	
	public static void processTask3 (float sup, Integer size, String username, String password) throws SQLException, IOException{
		
			String newline="\r\n";
		    // Load the Oracle JDBC driver
		    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

		    // Connect to the database
		    Connection conn =DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",username, password);
		    
		    task3IO = new PrintWriter(new BufferedWriter(new FileWriter("system.out.3")), true);
		    
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
		    	Task3.task3IO.println("{"+itemInfo1.getString(1)+"}, s="+((Float.parseFloat(itemInfo1.getString(2))*100)/totalTransInt)+"%");
		    
		    
		    //Create Candidate 2-Itemset Table and adding tuples
		    stmt.executeQuery("CREATE TABLE Cand2 (TRANSID INTEGER, ITEMID1 INTEGER, ITEMID2 INTEGER)");
		    stmt.executeQuery("INSERT INTO Cand2 SELECT p.TRANSID, p.ITEMID, q.ITEMID FROM Trans p, Trans q WHERE p.TRANSID=q.TRANSID AND q.ITEMID>p.ITEMID");
		    
		    //Create Frequent 2-Itemset Table and adding tuples
		    stmt.executeQuery("CREATE TABLE Freq2 (ITEMID1 INTEGER, ITEMID2 INTEGER, SUPPORT INTEGER)");
		    stmt.executeQuery("INSERT INTO Freq2 SELECT p.ITEMID1, p.ITEMID2, COUNT(*) FROM Cand2 p GROUP BY p.ITEMID1, p.ITEMID2 HAVING COUNT(*)>= "+minsup);
		    
		    //Join with Items and output in file
		    ResultSet itemInfo2=stmt.executeQuery("SELECT p.ITEMNAME as ITEM1, q.ITEMNAME as ITEM2, r.SUPPORT FROM ITEMS p, ITEMS q, Freq2 r WHERE r.ITEMID1=p.ITEMID AND r.ITEMID2=q.ITEMID");
		    while (itemInfo2.next ())
		    	Task3.task3IO.println("{"+itemInfo2.getString(1)+", "+itemInfo2.getString(2)+"}, s="+((Float.parseFloat(itemInfo2.getString(3))*100)/totalTransInt)+"%");
		    
		    
		    
		    //Create Optimization 2-Itemset table and adding tuples
		    stmt.executeQuery("CREATE TABLE Opt2 (TRANSID INTEGER, ITEMID1 INTEGER, ITEMID2 INTEGER)");
		    stmt.executeQuery("INSERT INTO Opt2 SELECT p.TRANSID, p.ITEMID1, p.ITEMID2 FROM Cand2 p, Freq2 q WHERE p.ITEMID1=q.ITEMID1 AND p.ITEMID2=q.ITEMID2");
		    
		    //Create table strings
		    String Tctrstr="TRANSID INTEGER, ITEMID1 INTEGER, ITEMID2 INTEGER";
		    String Ccrtstr="ITEMID1 INTEGER, ITEMID2 INTEGER";
		    String Rcrtstr="TRANSID INTEGER, ITEMID1 INTEGER, ITEMID2 INTEGER";
		    
		    String Tinstr="p.TRANSID, p.ITEMID1";
		    String Cinstr="p.ITEMID1, p.ITEMID2";
		    String Rinstr="p.TRANSID, p.ITEMID1, p.ITEMID2";
		    
		    String insR2="WHERE p.ITEMID1=q.ITEMID1 AND p.ITEMID2=q.ITEMID2";
		    String CrtT,crtC,crtR,insT,insC,insR1,insR; 
		    
		    //Create print strings
		    String printsel="q1.ITEMNAME, q2.ITEMNAME";
		    String printfrm=", ITEMS q1, ITEMS q2";
		    String printwhr="p.ITEMID1=q1.ITEMID AND p.ITEMID2=q2.ITEMID";
		    
		    for(int k=3;k<=size;k++){
		    	
		    	//Create Candidate k-ItemSet Table and adding tuples
		    	Tctrstr=Tctrstr+", ITEMID"+k+" INTEGER";
		    	CrtT="Create TABLE Cand"+(k)+"("+Tctrstr+")";
		    	stmt.executeQuery(CrtT);
		    
		    	Tinstr=Tinstr + ", p.ITEMID"+(k-1);
		    	insT="INSERT INTO Cand"+k+" SELECT "+Tinstr+", q.ITEMID FROM Opt"+(k-1)+" p, TRANS q WHERE q.TRANSID=p.TRANSID AND q.ITEMID>p.ITEMID"+(k-1);
		    	stmt.executeQuery(insT);
		    
		    	//Create Frequent k-ItemSet Table and adding tuples
		    	Ccrtstr=Ccrtstr+", ITEMID"+k+" INTEGER";
		    	crtC="CREATE TABLE Freq"+k+" ("+Ccrtstr+", SUPPORT INTEGER)";
		    	stmt.executeQuery(crtC);
		    
		    	Cinstr =Cinstr+", p.ITEMID"+k;
		    	insC="INSERT INTO Freq"+k+" SELECT "+Cinstr+", COUNT(*) FROM Cand"+k+" p GROUP BY "+Cinstr+" HAVING COUNT(*)>="+minsup; 
		    	stmt.executeQuery(insC);
		    	
		    	//Join with Items and output in file
		    	printsel =printsel + ", q"+k+".ITEMNAME ";
		    	printfrm =printfrm + ", ITEMS q"+k;
		    	printwhr =printwhr + " AND p.ITEMID"+k+" =q"+k+".ITEMID";
		    	
			    ResultSet itemInfo3=stmt.executeQuery("SELECT "+printsel+", p.SUPPORT FROM Freq"+k+" p"+printfrm+" WHERE "+printwhr+" ORDER BY p.SUPPORT");
			    while (itemInfo3.next ()){
			    	Task3.task3IO.print("{");
			    	for(int i=1;i<=k;i++)
			    		Task3.task3IO.print(itemInfo3.getString(i)+", ");
			    	Task3.task3IO.print("}, s="+((Float.parseFloat(itemInfo3.getString(k+1))*100)/totalTransInt)+"%");
			    	Task3.task3IO.println(newline);
			    }
		    
		    
		    	//Create Optimization k-ItemSet Table and adding tuples
		    	Rcrtstr=Rcrtstr+", ITEMID"+k+" INTEGER";
		    	crtR="CREATE TABLE Opt"+k+" ("+Rcrtstr+")";
		    	stmt.executeQuery(crtR);
		    
		    	Rinstr=Rinstr+", p.ITEMID"+k;
		    	insR1="INSERT INTO Opt"+k+" SELECT "+Rinstr+" FROM Cand"+k+" p, Freq"+k+" q ";
		    	insR2=insR2+" AND p.ITEMID"+k+"=q.ITEMID"+k;
		    	insR=insR1+insR2;
		    	stmt.executeQuery(insR);
		    
		    
		 
		    
		    }
		    
		    stmt.executeQuery("DROP TABLE Freq1");
		    for(int k=2;k<=size;k++){
		    	stmt.executeQuery("DROP TABLE Freq"+(k));
		    	stmt.executeQuery("DROP TABLE Cand"+k);
		    	stmt.executeQuery("DROP TABLE Opt"+k);
		    }
		    
		    conn.close(); // ** IMPORTANT : Close connections when done **
		  }

}
