package org.itmo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

@Slf4j
public class HDFSUploader {


    public void uploadFilesToHDFS() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:8020");
        conf.set("dfs.client.use.datanode.hostname", "true");


        try (FileSystem fs = FileSystem.get(conf)) {

            File localDir = new File("./");


            File[] csvFiles = localDir.listFiles(((dir, name) -> name.toLowerCase().endsWith(".csv")));

            Path pathInHDFSToCSVFiles = new Path("/input");

            if (!fs.exists(pathInHDFSToCSVFiles)) {
                fs.mkdirs(pathInHDFSToCSVFiles);
                log.info("Created HDFS directory: " + pathInHDFSToCSVFiles);
            }

            for (File csvFile : csvFiles) {

                String fileName = csvFile.getName();
                Path localPath = new Path(csvFile.getAbsolutePath());
                Path hdfsPath = new Path("/input/" + fileName);

                if (!fs.exists(hdfsPath)) {

                    fs.copyFromLocalFile(false, true, localPath, hdfsPath);
                    log.info("Copied HDFS file: " + hdfsPath);
                } else {
                    log.info("File already exists: " + hdfsPath);
                }


            }


        }
    }
}
