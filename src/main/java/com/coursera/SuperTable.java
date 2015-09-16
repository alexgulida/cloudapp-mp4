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
	   HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("superpowers"));
	   tableDescriptor.addFamily(new HColumnDescriptor("personal"));
	   tableDescriptor.addFamily(new HColumnDescriptor("professional"));
      // Add column families to table descriptor
	   tableDescriptor.addFamily(new HColumnDescriptor("hero"));
	   tableDescriptor.addFamily(new HColumnDescriptor("power"));
	   tableDescriptor.addFamily(new HColumnDescriptor("name"));
	   tableDescriptor.addFamily(new HColumnDescriptor("xp"));
      // Execute the table through admin
	   admin.createTable(tableDescriptor);
	   System.out.println(" Table created ");
      // Instantiating HTable class
	   HTable hTable = new HTable(con, "superpowers");
      // Repeat these steps as many times as necessary

	      // Instantiating Put class
              // Hint: Accepts a row name
	   Put p = new Put(Bytes.toBytes("row1"));
      	      // Add values using add() method
              // Hints: Accepts column family name, qualifier/row name ,value
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("hero"),Bytes.toBytes("superman"));
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("hero"),Bytes.toBytes("batman"));
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("hero"),Bytes.toBytes("volverine"));
	   
	   
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("powers"),Bytes.toBytes("strength"));
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("powers"),Bytes.toBytes("money"));
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("powers"),Bytes.toBytes("healing"));
	   
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("name"),Bytes.toBytes("clark"));
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("name"),Bytes.toBytes("bruce"));
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("name"),Bytes.toBytes("logan"));
	   
	   
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("xp"),Bytes.toBytes("100"));
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("xp"),Bytes.toBytes("50"));
	   p.add(Bytes.toBytes("personal"),Bytes.toBytes("xp"),Bytes.toBytes("75"));
	   
	   
      // Save the table
	   hTable.put(p);
	   System.out.println("data inserted");
      // Close table
	   hTable.close();
      // Instantiate the Scan class
	   HTable hTable2 = new HTable(con, "superpowers");
	   Scan scan = new Scan();
      // Scan the required columns
	   scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
	   
      // Get the scan result
	   ResultScanner scanner = hTable2.getScanner(scan);
	   
      // Read values from scan result
      // Print scan result
	   for (Result result = scanner.next(); result != null; result = scanner.next())
	    System.out.println("Found row : " + result);
      // Close the scanner
   scanner.close();
      // Htable closer
   hTable2.close();
   }
}

