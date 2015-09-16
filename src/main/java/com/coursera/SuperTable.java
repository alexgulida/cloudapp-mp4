package com.coursera;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{
	
   public static void main(String[] args) throws IOException {

      // Instantiate Configuration class
	   Configuration con = HBaseConfiguration.create();
      // Instaniate HBaseAdmin class
	   HBaseAdmin admin = new HBaseAdmin(con);
      // Instantiate table descriptor class
//	   HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("superpowers"));
	   HTableDescriptor tableDescriptor = new HTableDescriptor("powers");
	   tableDescriptor.addFamily(new HColumnDescriptor("personal"));
	   tableDescriptor.addFamily(new HColumnDescriptor("professional"));
      // Add column families to table descriptor
	   tableDescriptor.addFamily(new HColumnDescriptor("hero"));
	   tableDescriptor.addFamily(new HColumnDescriptor("power"));
	   tableDescriptor.addFamily(new HColumnDescriptor("name"));
	   tableDescriptor.addFamily(new HColumnDescriptor("xp"));
      // Execute the table through admin
	   admin.createTable(tableDescriptor);
//	   System.out.println(" Table created ");
      // Instantiating HTable class
	   HTable hTable = new HTable(con, "powers");
      // Repeat these steps as many times as necessary

	      // Instantiating Put class
              // Hint: Accepts a row name
	  
      	      // Add values using add() method
              // Hints: Accepts column family name, qualifier/row name ,value
	   
	   
	   String[][] matrix ={
			       {"superman","strength","clark","100"},
				   {"batman","money","bruce","50"},
				   {"wolverine","healing","logan","75"}
	   };
	   
	   int i = 1;
	  
	   for (String row[] : matrix){
		   		
	
		   	 	Put p = new Put(Bytes.toBytes("row"+i));
	   		 	p.add(Bytes.toBytes("personal"),Bytes.toBytes("hero"),Bytes.toBytes(row[0]));
			   	p.add(Bytes.toBytes("personal"),Bytes.toBytes("power"),Bytes.toBytes(row[1]));
			   	p.add(Bytes.toBytes("professional"),Bytes.toBytes("name"),Bytes.toBytes(row[2]));
			   	p.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"),Bytes.toBytes(row[3]));
			   	hTable.put(p);
			   	i++;
		   
	   }

      // Save the table
//	   hTable.put(p);
//	   System.out.println("data inserted");
      // Close table
	   hTable.close();
      // Instantiate the Scan class
	   HTable hTable2 = new HTable(con, "powers");
	   Scan scan = new Scan();
      // Scan the required columns
	   scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
	   
      // Get the scan result
	   ResultScanner scanner = hTable2.getScanner(scan);
	   
      // Read values from scan result
      // Print scan result
	   for (Result result = scanner.next(); result != null; result = scanner.next())
	    System.out.println(result);
      // Close the scanner
   scanner.close();
      // Htable closer
   hTable2.close();
   }
}

