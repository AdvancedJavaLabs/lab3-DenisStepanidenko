package org.itmo.service;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.itmo.Main;
import org.itmo.comparator.DescendingComparator;
import org.itmo.mapper.SalesMapper;
import org.itmo.mapper.SortMapper;
import org.itmo.model.SalesWritable;
import org.itmo.reduce.SalesReducer;
import org.itmo.reduce.SortReducer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class ExecuteService {

    private final Configuration configuration;
    private final FileSystem fileSystem;

    public ExecuteService(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.fileSystem = FileSystem.get(configuration);
    }

    public void execute(int numOfCsvFile, int countOfReduceNode) throws IOException, InterruptedException, ClassNotFoundException {

        deleteDirectories();

        Job aggregate = setUpAggreateJob(numOfCsvFile, countOfReduceNode);

        long startTime = System.currentTimeMillis();

        aggregate.waitForCompletion(true);

        Job sort = setUpSortJob();
        sort.waitForCompletion(true);

        long endTime = System.currentTimeMillis();

        downloadResultsFromHDFS(numOfCsvFile, countOfReduceNode, endTime - startTime);

    }

    public void closeFileSystem() throws IOException {
        fileSystem.close();
    }

    private void deleteDirectories() throws IOException {

        // удаляем папки, так как Hadoop требует, чтобы папки, куда мы записываем результаты не существовало перед запуском Job
        Path outputPath = new Path("/output");

        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

    }

    private Job setUpSortJob() throws IOException {

        Job job = Job.getInstance(configuration, "Сортировка по убыванию выручки.");
        job.setJarByClass(Main.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setNumReduceTasks(1);


        Path[] inputPaths = Arrays.stream(fileSystem.globStatus(new Path("/output/aggregateResult/part-r-*")))
                .map(FileStatus::getPath)
                .toArray(Path[]::new);

        FileInputFormat.setInputPaths(job, inputPaths);
        FileOutputFormat.setOutputPath(job, new Path("/output/sortResult"));

        job.setSortComparatorClass(DescendingComparator.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;

    }

    private Job setUpAggreateJob(int numOfCsvFile, int countOfReduceNode) throws IOException {

        Job job = Job.getInstance(configuration, "Анализ продаж по категориям.");
        job.setJarByClass(Main.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);

        job.setCombinerClass(SalesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SalesWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesWritable.class);

        job.setNumReduceTasks(countOfReduceNode);

        String pathToCsvFile = "/input/" + numOfCsvFile + ".csv";
        FileInputFormat.addInputPath(job, new Path(pathToCsvFile));
        FileOutputFormat.setOutputPath(job, new Path("/output/aggregateResult"));

        return job;

    }


    private void downloadResultsFromHDFS(int numOfCsvFile, int countOfReduceNode, long timeInMs) throws IOException {

        Path hdfsOutputPath = new Path("/output/sortResult");

        String localOutputDir = "results/";
        File localDir = new File(localOutputDir);

        if (!localDir.exists()) {
            localDir.mkdirs();
        }

        Path localDirPath = new Path("results/csv" + numOfCsvFile);
        File localDirFile = new File(localDirPath.toString());

        if (!localDirFile.exists()) {
            localDirFile.mkdirs();

            FileStatus[] hdfsFiles = fileSystem.listStatus(hdfsOutputPath);

            for (FileStatus fileStatus : hdfsFiles) {
                if (!fileStatus.isDirectory()) {
                    Path hdfsFilePath = fileStatus.getPath();
                    String fileName = hdfsFilePath.getName();
                    Path localFilePath = new Path(localDirPath, fileName);

                    fileSystem.copyToLocalFile(false, hdfsFilePath, localFilePath, true);
                }
            }
        }


        File resultTimeFile = new File(new File(localDirPath.toString()), "resultTimeFie.txt");
        Path hdfsPathToAggregationResult = new Path("/output/aggregateResult");

        Path localDirPathToAggregationResult = new Path("results/csv" + numOfCsvFile + "/aggregationResult/countOfReduceNode-" + countOfReduceNode);
        File localFilePathToAggregationResult = new File(localDirPathToAggregationResult.toString());

        if(!localFilePathToAggregationResult.exists()){
            localFilePathToAggregationResult.mkdirs();
        }

        FileStatus[] hdfsFilesAggregationResult = fileSystem.listStatus(hdfsPathToAggregationResult);

        for (FileStatus fileStatus : hdfsFilesAggregationResult) {
            if (!fileStatus.isDirectory()) {
                Path hdfsFilePath = fileStatus.getPath();
                String fileName = hdfsFilePath.getName();
                Path localFilePath = new Path(localDirPathToAggregationResult, fileName);

                fileSystem.copyToLocalFile(false, hdfsFilePath, localFilePath, true);
            }
        }


        String result = String.format("Count of reduce node = %d, time  = %.2f seconds. \n", countOfReduceNode, timeInMs / 1000.0);
        FileUtils.writeStringToFile(resultTimeFile, result, "UTF-8", true);

    }


}
