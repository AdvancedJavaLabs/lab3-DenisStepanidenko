package org.itmo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class Main {
    public static void main(String[] args) throws Exception {


        try {

            Configuration conf = new Configuration();


            conf.set("fs.defaultFS", "hdfs://localhost:8020");
            conf.set("dfs.client.use.datanode.hostname", "true");

            conf.set("hadoop.tmp.dir", "C:\\tmp\\hadoop");
            conf.set("mapreduce.cluster.local.dir",
                    "C:\\tmp\\hadoop\\local1,C:\\tmp\\hadoop\\local2");


            HDFSUploader uploader = new HDFSUploader();
            uploader.uploadFilesToHDFS(conf);


            Job job = Job.getInstance(conf, "Анализ продаж по категориям");
            job.setJarByClass(Main.class);
            job.setMapperClass(SalesMapper.class);
            job.setReducerClass(SalesReducer.class);

            job.setNumReduceTasks(10);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(SalesWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(SalesWritable.class);


            Path outputPath = new Path("/output");
            org.apache.hadoop.fs.FileSystem fs = outputPath.getFileSystem(conf);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }

            FileInputFormat.addInputPath(job, new Path("/input/0.csv"));
            FileOutputFormat.setOutputPath(job, new Path("/output"));

            boolean success = job.waitForCompletion(true);

            if(success){
                downloadResultsFromHDFS(conf);
            }

            System.exit(success ? 0 : 1);
        } catch (Throwable t){
            t.printStackTrace();
        }
    }


    private static void downloadResultsFromHDFS(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path hdfsOutputPath = new Path("/output");


        String localOutputDir = "results/";
        File localDir = new File(localOutputDir);
        if (!localDir.exists()) {
            localDir.mkdirs();
        }

        Path localDirPath = new Path("results/full_output");
        fs.copyToLocalFile(false, hdfsOutputPath, localDirPath, true);

        fs.close();
    }
}