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


public class Task4 {
	public static PrintWriter task4IO;
	
	public static void processTask4(float sup, double conf, int size, String username, String password) throws SQLException, IOException{
		
			String newline="\r\n";
		    // Load the Oracle JDBC driver
		    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

		    // Connect to the database
		    Connection conn =DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",username, password);
		    
		    task4IO = new PrintWriter(new BufferedWriter(new FileWriter("system.out.4")), true);
		    
		    // Create a Statement
		    Statement stmt = conn.createStatement ();
		    
		    //Calculate minimum support
		    float support=sup;
		    float totalTransInt=0;
		    ResultSet totaltrans = stmt.executeQuery ("SELECT COUNT(DISTINCT TRANSID) from Trans");
		    while (totaltrans.next ())
			   	  totalTransInt=Integer.parseInt(totaltrans.getString (1));
		    
		    double minsup =Math.ceil((support*totalTransInt)/100);
		    double minconf=conf;
		   // System.out.println(minsup);
		    
	
		    //Create Frequent 1-ItemSet Table and adding tuples
		    stmt.executeQuery("CREATE TABLE Freq1 (ITEMID1 INTEGER, SUPPORT INTEGER)");
		    stmt.executeQuery("INSERT INTO Freq1 SELECT ITEMID, COUNT(*) FROM TRANS HAVING COUNT(*)>= "+minsup+" GROUP BY ITEMID");
		    
		    //Create Candidate 2-Itemset Table and adding tuples
		    stmt.executeQuery("CREATE TABLE Cand2 (TRANSID INTEGER, ITEMID1 INTEGER, ITEMID2 INTEGER)");
		    stmt.executeQuery("INSERT INTO Cand2 SELECT p.TRANSID, p.ITEMID, q.ITEMID FROM Trans p, Trans q WHERE p.TRANSID=q.TRANSID AND q.ITEMID!=p.ITEMID");
		    
		    //Create Frequent 2-Itemset Table and adding tuples
		    stmt.executeQuery("CREATE TABLE Freq2 (ITEMID1 INTEGER, ITEMID2 INTEGER, SUPPORT INTEGER)");
		    stmt.executeQuery("INSERT INTO Freq2 SELECT p.ITEMID1, p.ITEMID2, COUNT(*) FROM Cand2 p GROUP BY p.ITEMID1, p.ITEMID2 HAVING COUNT(*)>= "+minsup);
		    
		    //Create Association Rules 2-Itemset Table and adding tuples
		    stmt.executeQuery("CREATE TABLE FreqAR2(ITEMID1 INTEGER,ITEMID2 INTEGER, SUPPORT INTEGER, CONFIDENCE FLOAT(10))");
		    stmt.executeQuery("INSERT INTO FreqAR2 SELECT p.ITEMID1,p.ITEMID2,p.SUPPORT,p.SUPPORT/q.SUPPORT FROM Freq2 p,Freq1 q WHERE p.ITEMID1=q.ITEMID1");
		    
		    //Find Rules >confidence and output
		    ResultSet itemInfo2=stmt.executeQuery("SELECT p.ITEMNAME as ITEM1, q.ITEMNAME as ITEM2, r.SUPPORT, r.CONFIDENCE FROM ITEMS p, ITEMS q, FreqAR2 r WHERE r.ITEMID1=p.ITEMID AND r.ITEMID2=q.ITEMID AND r.CONFIDENCE>="+minconf);
		    while (itemInfo2.next ())
		    	Task4.task4IO.println("{{"+itemInfo2.getString(1)+"} - > {"+itemInfo2.getString(2)+"}}, s="+((Float.parseFloat(itemInfo2.getString(3))*100)/totalTransInt)+"%, c="+((Float.parseFloat(itemInfo2.getString(4))*100))+"%"+newline);
		    
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
		    
		    String insT2="WHERE p.TRANSID=q.TRANSID AND q.ITEMID!=p.ITEMID1";
		    String insR2="WHERE p.ITEMID1=q.ITEMID1 AND p.ITEMID2=q.ITEMID2";
		    String CrtT,crtC,crtR,insT,insT1,insC,insR1,insR;
		    
		    
		    //Create Association Rules Strings
		    String ARcrt=" ITEMID1 INTEGER,ITEMID2 INTEGER";
		    String ARsel=" p.ITEMID1, p.ITEMID2 ";
		    String ARwhr1=" p.ITEMID1=q.ITEMID1";
		    String ARwhr2=" ";
		    String ARwhr3=" ";
		    String crtAR,insAR;
		    
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
		    		insT1="INSERT INTO Cand"+k+" SELECT "+Tinstr+", q.ITEMID FROM Opt"+(k-1)+" p, TRANS q ";
		    		insT2=insT2 + " AND q.ITEMID!=p.ITEMID"+(k-1);
		    		insT=insT1 + insT2;
		    		stmt.executeQuery(insT);
			    
		    		//Create Frequent k-ItemSet Table and adding tuples
		    		Ccrtstr=Ccrtstr+", ITEMID"+k+" INTEGER";
		    		crtC="CREATE TABLE Freq"+k+" ("+Ccrtstr+", SUPPORT INTEGER)";
		    		stmt.executeQuery(crtC);
			    
		    		Cinstr =Cinstr+", p.ITEMID"+k;
		    		insC="INSERT INTO Freq"+k+" SELECT "+Cinstr+", COUNT(*) FROM Cand"+k+" p GROUP BY "+Cinstr+" HAVING COUNT(*)>="+minsup; 
		    		stmt.executeQuery(insC);
		    		
		    		//Create Association Rules k-Itemset Table and adding tuples
		    		ARcrt= ARcrt + ", ITEMID"+k+" INTEGER"; 
		    		ARsel= ARsel + ", p.ITEMID"+k;
		    		
		    		//Join with Items and output in file
			    	printsel =printsel + ", q"+k+".ITEMNAME ";
			    	printfrm =printfrm + ", ITEMS q"+k;
			    	printwhr =printwhr + " AND p.ITEMID"+k+" =q"+k+".ITEMID";
			    	
			    	ARwhr1=" p.ITEMID1=q.ITEMID1";
		    		for(int i=1;i<k;i++){
		    			ARwhr1= ARwhr1 + " AND p.ITEMID"+i+" =q.ITEMID"+i;
		    			crtAR="CREATE TABLE FreqAR"+k+"s"+i+" ("+ARcrt+", SUPPORT INTEGER, CONFIDENCE FLOAT(10))";
		    			stmt.executeQuery(crtAR);
		    			
		    			ARwhr2=" ";
		    			for(int j=2;j<=i;j++){
		    				ARwhr2= ARwhr2 + " AND q.ITEMID"+(j-1)+"<q.ITEMID"+(j);
		    			}
		    			
		    			ARwhr3=" ";
		    			for(int j=i+2;j<=k;j++){
		    				ARwhr3= ARwhr3 + " AND p.ITEMID"+(j-1)+"<p.ITEMID"+(j);
		    			}
		    				
		    			insAR="INSERT INTO FreqAR"+k+"s"+i+" SELECT "+ARsel+", p.SUPPORT, p.SUPPORT/q.SUPPORT FROM Freq"+k+" p, Freq"+i+" q WHERE"+ARwhr1+ARwhr2+ARwhr3;
		    			//System.out.println(insAR);
		    			stmt.executeQuery(insAR);
		    			
		    			
				    	
				    	String TEST="SELECT "+printsel+", p.SUPPORT, p.CONFIDENCE FROM FreqAR"+k+"s"+i+" p"+printfrm+" WHERE "+printwhr+" AND p.CONFIDENCE>="+minconf;
				    	ResultSet itemInfo3=stmt.executeQuery(TEST);
					   
				    	while (itemInfo3.next ()){
					    	Task4.task4IO.print("{{");
					    	for(int m=1;m<=i;m++)
					    		{
					    		if (m!=i)
					    			Task4.task4IO.print(itemInfo3.getString(m)+", ");
					    		else 
					    			Task4.task4IO.print(itemInfo3.getString(m));
					    		}
					    		
					    	Task4.task4IO.print("} - > {");
					    	for(int m=i+1;m<=k;m++)
					    		{
					    		if (m!=k)
					    			Task4.task4IO.print(itemInfo3.getString(m)+", ");
					    		else 
					    			Task4.task4IO.print(itemInfo3.getString(m));
					    		}
					    	Task4.task4IO.print("}}, s="+((Float.parseFloat(itemInfo3.getString(k+1))*100)/totalTransInt)+"% c="+((Float.parseFloat(itemInfo3.getString(k+2))*100))+"%");
					    	Task4.task4IO.println(newline);
					    }
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
		    stmt.executeQuery("DROP TABLE FreqAR2");
		    for(int k=2;k<=size;k++){
		    	stmt.executeQuery("DROP TABLE Freq"+(k));
		    	stmt.executeQuery("DROP TABLE Cand"+k);
		    	stmt.executeQuery("DROP TABLE Opt"+k);
		    }
		    for(int k=3;k<=size;k++){
		    	for(int i=1;i<k;i++)
		    	stmt.executeQuery("DROP TABLE FreqAR"+k+"s"+i);
		    }
		    conn.close(); // ** IMPORTANT : Close connections when done **
		  }

}
