package org.itmo.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length >= 2) {
            String category = parts[0];
            String data = parts[1];

            double revenue = extractRevenue(data);

            DoubleWritable outputKey = new DoubleWritable(-revenue);

            Text outputValue = new Text(category + "\t" + data);

            context.write(outputKey, outputValue);
        }
    }

    private double extractRevenue(String data) {

        try {
            int start = data.indexOf("revenue:") + 8;
            String revenueStr = data.substring(start).trim().replace(",", ".");
            return Double.parseDouble(revenueStr);
        } catch (Exception e) {
            return 0.0;
        }

    }
}