package org.itmo;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.itmo.service.ExecuteService;
import org.itmo.service.HDFSUploader;

@Slf4j
public class Main {


    public static void main(String[] args) throws Exception {


        try {

            Configuration configuration = setUpConf();

            HDFSUploader uploader = new HDFSUploader();
            uploader.uploadFilesToHDFS(configuration);

            ExecuteService executeService = new ExecuteService(configuration);

            // здесь мы переберём все csv файлы и соберём статистику по масштабированию для каждого csv
            for (int numOfCsvFile = 0; numOfCsvFile <= 7; numOfCsvFile++) {

                for (int countOfReduceNode = 1; countOfReduceNode <= 20; countOfReduceNode++) {

                    executeService.executeCSVFile(numOfCsvFile, countOfReduceNode);
                }

            }

            // обработаем сразу все файлы
            for (int countOfReduceNode = 1; countOfReduceNode <= 20; countOfReduceNode++) {

                executeService.executeAllCSVFile(countOfReduceNode);

            }


            executeService.closeFileSystem();

        } catch (Throwable t) {

            log.error("Произошла ошибка {}", t.getMessage());
            throw new RuntimeException(t);

        }
    }

    /**
     * Конфигурация для клиента Hadoop
     */
    private static Configuration setUpConf() {

        System.setProperty("java.library.path", "C:\\hadoop-3.3.6\\bin");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:8020");
        configuration.set("dfs.client.use.datanode.hostname", "true");


        return configuration;
    }

}