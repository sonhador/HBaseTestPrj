package sonhador.hbasetest;

import java.awt.Color;
import java.awt.image.BufferedImage;
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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author asonh
 */
public class Main {
    public static void main(String []args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.err.println("Correct Usage: <repetitions>");
            System.exit(1);
        }
        
        new Main(Integer.parseInt(args[0]));
    }
    
    public Main(int repetitions) throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        
        TableName rgbTable = TableName.valueOf("rgbTable");
        String rFamily = "r";
        String gFamily = "g";
        String bFamily = "b";

        System.out.println("Admin conn attempt..");
        
        /**
         * using HBase Master
         */
        Connection conn = ConnectionFactory.createConnection(config);
        Admin admin = conn.getAdmin();
        
        System.out.println("Admin conn ok..");
        
        HTableDescriptor desc = new HTableDescriptor(rgbTable);
        desc.addFamily(new HColumnDescriptor(rFamily).setCompressTags(true).setCompressionType(Compression.Algorithm.GZ));
        desc.addFamily(new HColumnDescriptor(gFamily).setCompressTags(true).setCompressionType(Compression.Algorithm.GZ));
        desc.addFamily(new HColumnDescriptor(bFamily).setCompressTags(true).setCompressionType(Compression.Algorithm.GZ));
        
        try {
            System.out.println("Deleting table..");
            admin.disableTable(rgbTable);
            admin.deleteTable(rgbTable);
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
        
        Table rgbHTable = conn.getTable(rgbTable);
        
        System.out.println("Table conn ok..");
        
        BufferedImage image = ImageIO.read(getClass().getResourceAsStream("/white.bmp"));
        CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
        
        int wWidth = String.format("%d", image.getWidth()).length();
        int hWidth = String.format("%d", image.getHeight()).length();
        
        List<Put> puts = new ArrayList<>();
        for (int i=0; i<repetitions; i++) {
            byte[] row = Bytes.toBytes(i);
            Put put = new Put(row);
            for (int w=0; w<image.getWidth(); w++) {
                int wZeroPadding = wWidth - String.format("%d", w).length();
                for (int h=0; h<image.getHeight(); h++) {
                    int hZeroPadding = hWidth - String.format("%d", h).length();
                    
                    System.out.println(w+","+h);
                    
                    Color pixel = new Color(image.getRGB(w, h));
                    
                    Cell rCell = CellUtil.createCell(row, 
                                                     rFamily.getBytes(), 
                                                     Bytes.toBytes(w+","+h), 
                                                     System.currentTimeMillis(), 
                                                     (byte)4,
                                                     Bytes.toBytes(pixel.getRed()));
                    
                    Cell gCell = CellUtil.createCell(row, 
                                                     gFamily.getBytes(), 
                                                     Bytes.toBytes(w+","+h), 
                                                     System.currentTimeMillis(), 
                                                     (byte)4,
                                                     Bytes.toBytes(pixel.getGreen()));
                    
                    Cell bCell = CellUtil.createCell(row, 
                                                     bFamily.getBytes(), 
                                                     Bytes.toBytes(w+","+h), 
                                                     System.currentTimeMillis(), 
                                                     (byte)4,
                                                     Bytes.toBytes(pixel.getBlue()));
                    
                    put.add(rCell).add(gCell).add(bCell);
                }
            }
            puts.add(put);
        }
        rgbHTable.put(puts);
        conn.close();
    }
}