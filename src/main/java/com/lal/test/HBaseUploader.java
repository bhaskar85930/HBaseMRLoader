package com.lal.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Sreejithlal G S
 * @since 15-Aug-2016
 * 
 */
public class HBaseUploader extends Configured implements Tool {

    private static final String NAME = "HBASE_UPLOADER";

    static class Uploader extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private String[] columns = { "duration", "cost", "state" };
        private byte[] family = Bytes.toBytes("call");
        private int count = 0, checkPoint = 100;

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException {

            String[] values = line.toString().split(",");
            if (values.length < 4) {
                System.out.println("ERR: number of colums is < 4 len: " + line);
                return;
            }
            
            // To Ignore first row of file
            
            if (values[0].equalsIgnoreCase("#call_uuid")) {
                return;
            }
            byte[] rowKey = Bytes.toBytes(values[0]);
            Put put = new Put(rowKey);
            for (int i = 1; i < 4; i++) {
                put.addColumn(family, Bytes.toBytes(columns[i - 1]), Bytes.toBytes(values[i]));

            }
            try {
                context.write(new ImmutableBytesWritable(rowKey), put);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (++count % checkPoint == 0)
                context.setStatus("Emitting Put " + count);
        }
    }

    /**
     * Job configuration.
     */
    public static Job configureJob(Configuration conf, String[] args) throws IOException {
        Path inputPath = new Path(args[0]);
        String tableName = args[1];
        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJarByClass(Uploader.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Uploader.class);
        // No reducers. Just write straight to table. Call initTableReducerJob
        // because it sets up the TableOutputFormat.
        TableMapReduceUtil.initTableReducerJob(tableName, null, job);
        job.setNumReduceTasks(0);
        return job;
    }

    /**
     * Main entry point.
     *
     * @param otherArgs
     *            The command line parameters after ToolRunner handles standard.
     * @throws Exception
     *             When running the job fails.
     */
    public int run(String[] otherArgs) throws Exception {
        if (otherArgs.length != 2) {
            System.err.println("Wrong number of arguments: " + otherArgs.length);
            System.err.println("Usage: " + NAME + " <input> <tablename>");
            return -1;
        }
        Job job = configureJob(getConf(), otherArgs);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        
        //
        // create environment variable HBASE_CONF_DIR, which points to configuration directory of hbase.
        //
        
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        int status = ToolRunner.run(conf, new HBaseUploader(), args);
        System.exit(status);
    }

}
