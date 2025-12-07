package org.itmo.reduce;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.itmo.model.SalesWritable;

import java.io.IOException;

@Slf4j
public class SalesReducer extends Reducer<Text, SalesWritable, Text, SalesWritable> {


    private SalesWritable result = new SalesWritable();

    @Override
    protected void reduce(Text key, Iterable<SalesWritable> values, Reducer<Text, SalesWritable, Text, SalesWritable>.Context context) throws IOException, InterruptedException {


        double totalRevenue = 0.0;
        int totalQuantity = 0;

        log.debug("Обработка reduce значений {}", values);


        for (SalesWritable val : values) {
            totalRevenue += val.getRevenue();
            totalQuantity += val.getQuantity();

        }

        result.setRevenue(totalRevenue);
        result.setQuantity(totalQuantity);

        context.write(key, result);
    }

    @Override
    protected void setup(Reducer<Text, SalesWritable, Text, SalesWritable>.Context context) throws IOException, InterruptedException {
        log.debug("Mapper started with configuration: {}", context.getConfiguration().get("fs.defaultFS"));
    }


}
