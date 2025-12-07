package org.itmo.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * Объект для загрузки файлов в HDFS
 */
@Slf4j
public class HDFSUploader {

    public void uploadFilesToHDFS(Configuration conf) throws IOException {

        try (FileSystem fs = FileSystem.get(conf)) {

            File localDir = new File("./input_csv");

            File[] csvFiles = localDir.listFiles();

            Path pathInHDFSToCSVFiles = new Path("/input");

            if (!fs.exists(pathInHDFSToCSVFiles)) {
                fs.mkdirs(pathInHDFSToCSVFiles);
                log.debug("Created HDFS directory: {}", pathInHDFSToCSVFiles);
            } else {
                log.debug("Directory already exists: {}", pathInHDFSToCSVFiles);
            }

            for (File csvFile : csvFiles) {

                String fileName = csvFile.getName();
                Path localPath = new Path(csvFile.getAbsolutePath());
                Path hdfsPath = new Path("/input/" + fileName);

                if (!fs.exists(hdfsPath)) {

                    fs.copyFromLocalFile(false, true, localPath, hdfsPath);
                    log.debug("Create HDFS file: {}", hdfsPath);

                } else {
                    log.debug("File already exists: {}", hdfsPath);
                }

            }

        }

    }

}
