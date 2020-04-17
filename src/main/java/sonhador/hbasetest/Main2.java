/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sonhador.hbasetest;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.imageio.ImageIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author asonh
 */
public class Main2 {
    public static void main(String []args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.err.println("Correct Usage: <repetitions>");
            System.exit(1);
        }
        
        new Main2(Integer.parseInt(args[0]));
    }
    
    public Main2(int repetitions) throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        
        TableName bmpTable = TableName.valueOf("bmpTable");
        String dataFamily = "data";

        System.out.println("Admin conn attempt..");
        
        /**
         * using HBase Master
         */
        Connection conn = ConnectionFactory.createConnection(config);
        Admin admin = conn.getAdmin();
        
        System.out.println("Admin conn ok..");
        
        HTableDescriptor desc = new HTableDescriptor(bmpTable);
        desc.addFamily(new HColumnDescriptor(dataFamily).setCompressTags(true).setCompressionType(Compression.Algorithm.GZ));
        
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
        
        Table bmpHTable = conn.getTable(bmpTable);
        
        System.out.println("Table conn ok..");
        
        BufferedImage image = ImageIO.read(getClass().getResourceAsStream("/white.bmp"));
        
        List<Put> puts = new ArrayList<>();
        for (int i=0; i<repetitions; i++) {
            byte[] row = Bytes.toBytes(i);
            Put put = new Put(row);
            
            System.out.println(i);
            
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            ImageIO.write(image, "bmp", buf);
            
            Cell cell = CellUtil.createCell(row, 
                                             dataFamily.getBytes(), 
                                             Bytes.toBytes("data"), 
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
        bmpHTable.close();
        conn.close();
    }
}