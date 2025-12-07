package org.itmo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Преобразует строки CSV файлов в пары ключ-значение
 * extends Mapper<ВХОДНОЙ_КЛЮЧ, ВХОДНОЕ_ЗНАЧЕНИЕ, ВЫХОДНОЙ_КЛЮЧ, ВЫХОДНОЕ_ЗНАЧЕНИЕ>
 */
@Slf4j
public class SalesMapper extends Mapper<LongWritable, Text, Text, SalesWritable> {

    /**
     * Ключ на выходе
     */
    private Text category = new Text();

    /**
     * Данные о продаже на выходе
     */
    private SalesWritable dataSales = new SalesWritable();

    /**
     * Метод, вызываемый для каждой строки для преобразования строки в пару ключ-значение для последующей отправки в reduce-узел
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, SalesWritable>.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        System.out.println("Обработка строки в mapper: " +  line);

        if (line.startsWith("transaction_id") || line.trim().isEmpty()) {
            return;
        }

        String[] parts = line.split(",");
        // transaction_id,product_id,category,price,quantity

        String categoryName = parts[2];
        double price = Double.parseDouble(parts[3]);
        int quantity = Integer.parseInt(parts[4]);

        category.set(categoryName);

        dataSales.setQuantity(quantity);
        dataSales.setRevenue(quantity * price);

        context.write(category, dataSales);

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Mapper started with configuration: " +
                context.getConfiguration().get("fs.defaultFS"));
    }

}
