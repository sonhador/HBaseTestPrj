/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sonhador.hbasetest;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import javax.imageio.ImageIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/**
 *
 * @author asonh
 */
public class App {
    public static void main(String []args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.err.println("Correct Usage: <repetitions>");
            System.exit(1);
        }
        
        new App(Integer.parseInt(args[0]));
    }
    
    public App(int repetitions) throws IOException, InterruptedException {
//        Configuration config = HBaseConfiguration.create();
//        config.addResource(getClass().getResourceAsStream("hbase-site.xml"));
        
        TableName rgbTable = TableName.valueOf("rgbTable");
        String rFamily = "r";
        String gFamily = "g";
        String bFamily = "b";

        /**
         * using HBase Master
         */
//        Connection conn = ConnectionFactory.createConnection(config);
//        Admin admin = conn.getAdmin();
//        
//        HTableDescriptor desc = new HTableDescriptor(rgbTable);
//        desc.addFamily(new HColumnDescriptor(rFamily));
//        desc.addFamily(new HColumnDescriptor(gFamily));
//        desc.addFamily(new HColumnDescriptor(bFamily));
//        
//        if (admin.isTableAvailable(rgbTable) == false) {
//            admin.createTable(desc);
//        }
//        
//        Table rgbHTable = conn.getTable(rgbTable);
        
        /**
         * using HFile generation
         */
        Configuration conf = new Configuration();
        conf.set("mapred.working.dir", "file:///tmp");
        conf.set("mapred.output.dir", "file:///tmp/hFiles");
        conf.setBoolean("hbase.bulkload.locality.sensitive.enabled", false);
        conf.setBoolean("hbase.regionserver.wal.enablecompression", true);
        conf.setLong("hbase.mapreduce.hfileoutputformat.blocksize", 1048576*500);
        conf.set("hbase.mapreduce.hfileoutputformat.table.name", rgbTable.getNameAsString());
        conf.set("hbase.mapreduce.hfileoutputformat.compression", "gzip");
        conf.set("hbase.hfileoutputformat.families.compression", "gzip");
        
        HFileOutputFormat2 outputFormat = new HFileOutputFormat2();
        TaskAttemptContext ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID(new TaskID(new JobID("test", 1), TaskType.REDUCE, 1), 1));
        RecordWriter writer = outputFormat.getRecordWriter(ctx);
        
        BufferedImage image = ImageIO.read(getClass().getResourceAsStream("/accord_house.bmp"));
        CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
        
        int wWidth = String.format("%d", image.getWidth()).length();
        int hWidth = String.format("%d", image.getHeight()).length();
        
        for (int i=0; i<repetitions; i++) {
            byte[] row = Bytes.toBytes(i);
//            Put put = new Put(row);
            for (int w=0; w<image.getWidth(); w++) {
                int wZeroPadding = wWidth - String.format("%d", w).length();
                for (int h=0; h<image.getHeight(); h++) {
                    int hZeroPadding = hWidth - String.format("%d", h).length();
                    
                    Color pixel = new Color(image.getRGB(w, h));
                    
                    /**
                     * using HBase Master
                     */                    
//                    put.addImmutable(rFamily.getBytes(), Bytes.toBytes(w+","+h), Bytes.toBytes(pixel.getRed()));
//                    put.addImmutable(gFamily.getBytes(), Bytes.toBytes(w+","+h), Bytes.toBytes(pixel.getGreen()));
//                    put.addImmutable(bFamily.getBytes(), Bytes.toBytes(w+","+h), Bytes.toBytes(pixel.getBlue()));
//                    rgbHTable.put(put);

                    byte []qualifier = (getZeroPadding(wZeroPadding)+w+","+getZeroPadding(hZeroPadding)+h).getBytes();

                    writer.write(new ImmutableBytesWritable(rgbTable.getName()), createCell(row, rFamily.getBytes(), qualifier, Bytes.toBytes(pixel.getRed()), cellBuilder));
                    writer.write(new ImmutableBytesWritable(rgbTable.getName()), createCell(row, gFamily.getBytes(), qualifier, Bytes.toBytes(pixel.getGreen()), cellBuilder));
                    writer.write(new ImmutableBytesWritable(rgbTable.getName()), createCell(row, bFamily.getBytes(), qualifier, Bytes.toBytes(pixel.getBlue()), cellBuilder));
                }
            }
        }
        writer.close(ctx);
//        conn.close();
    }
    
    private String getZeroPadding(int paddingLen) {
        StringBuffer buf = new StringBuffer();
        for (int i=0; i<paddingLen; i++) {
            buf.append("0");
        }
        
        return buf.toString();
    }
    
    private KeyValue createCell(byte []row, byte []family, byte []qualifier, byte []value, CellBuilder builder) {
        builder.setRow(row);
        builder.setFamily(family);
        builder.setQualifier(qualifier);
        builder.setValue(value);
        builder.setType(Cell.Type.Put);
        
        Cell cell = builder.build();
        KeyValue kv = new KeyValue(cell);
        
        return kv;
    }
}
