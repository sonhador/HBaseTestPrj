/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sonhador.hbasetest;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.imageio.ImageIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author asonh
 */
public class Main {
    public static void main(String []args) throws IOException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Correct Usage: <HBASE_CONF_PATH> <IMAGE_PATH> <UPLOAD_REPETITIONS>");
            System.exit(1);
        }
        
        new Main(args[0], args[1], Integer.parseInt(args[2]));
    }
    
    public Main(String hbaseConfPath, String imagePath, int repetitions) throws IOException, InterruptedException {
        HBaseConfiguration config = new HBaseConfiguration(new Configuration());
        config.addResource(new Path(hbaseConfPath));
        
        TableName bmpTable = TableName.valueOf("bmpTable");
        String columnFamily = "bmpImage";
        
        HTableDescriptor desc = new HTableDescriptor(bmpTable);
        desc.addFamily(new HColumnDescriptor(columnFamily).setCompressTags(true).setCompressionType(Compression.Algorithm.GZ));

        System.out.println("Admin conn attempt..");
        
        /**
         * using HBase Master
         */
        HBaseAdmin admin = new HBaseAdmin(config);
        
        System.out.println("Admin conn ok..");
        
        try {
            System.out.println("Deleting table..");
            admin.disableTable(bmpTable);
            admin.deleteTable(bmpTable);
            System.out.println("Deleting table ok..");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        
        try {
            System.out.println("Creating table..");
            admin.createTable(desc);
            System.out.println("Creating table ok..");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        
        HConnection conn = HConnectionManager.createConnection(config);
        HTableInterface bmpHTable = conn.getTable(bmpTable);
        
        System.out.println("Table conn ok..");
        
        BufferedImage image = ImageIO.read(new File(imagePath));
        
        List<Put> puts = new ArrayList<>();
        for (int i=0; i<repetitions; i++) {
            byte[] row = Bytes.toBytes(i);
            Put put = new Put(row);
            
            System.out.println(i + " " + String.format("%,d bytes", (i+1)*(new File(imagePath).length())));
            
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            ImageIO.write(image, "bmp", buf);
            
            Cell cell = CellUtil.createCell(row, 
                                             columnFamily.getBytes(), 
                                             Bytes.toBytes("bmpBytes"), 
                                             System.currentTimeMillis(), 
                                             (byte)4,
                                             buf.toByteArray());
            
            put.add(cell);
            puts.add(put);
            
            if (i % 100 == 0) {
                bmpHTable.put(puts);
                puts = new ArrayList<>();
            }
        }
        
        if (puts.size() > 0) {
            bmpHTable.put(puts);
        }
        admin.compact(bmpTable.getName());
        bmpHTable.close();
        admin.close();
        conn.close();
    }
}