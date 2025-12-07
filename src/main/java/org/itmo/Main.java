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

            // здесь мы переберём все csv файлы и соберём статистику по масштабированию
            for (int numOfCsvFile = 0; numOfCsvFile <= 7; numOfCsvFile++) {

                for (int countOfReduceNode = 1; countOfReduceNode <= 20; countOfReduceNode++) {

                    executeService.execute(numOfCsvFile, countOfReduceNode);
                }

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

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:8020");
        configuration.set("dfs.client.use.datanode.hostname", "true");

        // КРИТИЧЕСКИ ВАЖНЫЕ ОПТИМИЗАЦИИ:

        // 1. Для локального режима
        configuration.set("mapreduce.framework.name", "local");
        configuration.setBoolean("mapreduce.jobtracker.address", false);

        // 2. Оптимизация памяти
        configuration.set("mapreduce.map.memory.mb", "1024");
        configuration.set("mapreduce.reduce.memory.mb", "1024");
        configuration.set("mapreduce.map.java.opts", "-Xmx768m");
        configuration.set("mapreduce.reduce.java.opts", "-Xmx768m");

        // 3. Сжатие (ускоряет shuffle)
        configuration.setBoolean("mapred.compress.map.output", true);
        configuration.set("mapred.map.output.compression.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");

        // 4. Параметры сортировки/слияния
        configuration.setInt("mapreduce.task.io.sort.mb", 200);
        configuration.setInt("mapreduce.task.io.sort.factor", 100);
        configuration.setInt("mapreduce.reduce.shuffle.parallelcopies", 50);

        // 5. Для маленьких/средних файлов
        configuration.setBoolean("mapreduce.job.ubertask.enable", true);
        configuration.setLong("mapreduce.input.fileinputformat.split.minsize", 1L);

        // 6. Оптимизация вывода
        configuration.setBoolean("mapreduce.output.fileoutputformat.compress", false);

        return configuration;
    }

}