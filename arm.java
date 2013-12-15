import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


public class arm {

	public static void main(String[] args) throws SQLException, IOException{
		// Load the Oracle JDBC driver
		int k=3;
		k++;
	    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		
		//Read System.in file
		 BufferedReader in = new BufferedReader(new FileReader("system.in.dat"));
		 String line1=in.readLine();
		 String[] cred=line1.split(" ");
		 String username=cred[2].replaceAll("\\,","");
		 String password=cred[5];

	    // Connect to the database
	    Connection conn =DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",username, password);
	    
	    
	    Statement stmt = conn.createStatement ();
	    //Create tables Trans, Items
	    String CreateTrans="CREATE TABLE Trans(TRANSID INTEGER, ITEMID INTEGER)";
	    String CreateItems="CREATE TABLE Items(ITEMID INTEGER, ITEMNAME VARCHAR(40))";
	    stmt.executeQuery(CreateItems);
	    stmt.executeQuery(CreateTrans);
	    
	    
	    String InsertTrans = "INSERT INTO Trans(TRANSID,ITEMID) VALUES(?,?)";
	    String InsertItems = "INSERT INTO Items(ITEMID, ITEMNAME) VALUES(?,?)";
	    
	    PreparedStatement ps1 = conn.prepareStatement(InsertTrans);
	    PreparedStatement ps2 = conn.prepareStatement(InsertItems);
	    
	    // Adding tuples in trans table
	    BufferedReader br1 = new BufferedReader(new FileReader("trans.dat"));			
		String[] stringArray1=new String[2];
		String string1 = br1.readLine();													
		
		while (string1 != null){
			int transid,itemid;
			stringArray1 = string1.split(",");	
			
			try {
				transid = Integer.parseInt(stringArray1[0]);													
				itemid = Integer.parseInt(stringArray1[1]);
				
				ps1.setInt(1, transid);
			    ps1.setInt(2, itemid);
				ps1.addBatch();
				
				string1=br1.readLine();
			}
			
			catch (Exception e){
				break;
			}
		}
		ps1.executeBatch();
		ps1.close();
		
		//Adding tuples in items
		BufferedReader br2 = new BufferedReader(new FileReader("items.dat"));			
		String[] stringArray2=new String[2];
		String string2 = br2.readLine();													
				
		while (string2 != null){
			int itemid;
			String itemname;
			stringArray2 = string2.split(",");	
					
			try {												
				itemid = Integer.parseInt(stringArray2[0]);
				itemname=stringArray2[1];
						
				ps2.setInt(1, itemid);
			    ps2.setString(2, itemname);
				ps2.addBatch();
				string2=br2.readLine();
				}
			catch (Exception e){
				break;}
		}
		ps2.executeBatch();
		ps2.close();
		conn.close();
		
		Task1 task1=new Task1();
		Task2 task2=new Task2();
		Task3 task3=new Task3();
		Task4 task4=new Task4();
		
		
		//Read parameters
		String line2=in.readLine();
		String[] s2=line2.split(" ");
		float suptask1=Float.parseFloat(s2[3].replaceAll("\\%",""));
	
		
		String line3=in.readLine();
		String[] s3=line3.split(" ");
		float suptask2=Float.parseFloat(s3[3].replaceAll("\\%",""));
	
		
		String line4=in.readLine();
		String[] s4=line4.split(" ");
		float suptask3=Float.parseFloat(s4[3].replaceAll("\\%\\,",""));
		int size3=Integer.parseInt(s4[6]);
		
		
		String line5=in.readLine();
		String[] s5=line5.split(" ");
		float suptask4=Float.parseFloat(s5[3].replaceAll("\\%\\,",""));
		double confidence=(Double.parseDouble(s5[6].replaceAll("\\%\\,","")))/100;
		int size4=Integer.parseInt(s5[9]);
		
		
		task1.processTask1(suptask1,username,password);
		task2.processTask2(suptask2,username,password);
		task3.processTask3(suptask3,size3,username,password);
		task4.processTask4(suptask4, confidence, size4, username, password);
		
		
		
		DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
	    Connection conn1 =DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",username, password);
	    Statement stmt1 = conn1.createStatement ();
	    stmt1.executeQuery("DROP TABLE Trans");
	    stmt1.executeQuery("DROP TABLE Items");
	    conn1.close();
		
	}
}
