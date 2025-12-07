package org.itmo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Объект для хранения данных и продажах
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class SalesWritable implements Writable {

    private double revenue;
    private int quantity;


    /**
     * Hadoop использует этот метод, когда хочет записать объект на диск
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeDouble(this.revenue);
        dataOutput.writeInt(this.quantity);
    }

    /**
     * Hadoop использует этот метод, когда хочет прочитать объект с диска
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.revenue = dataInput.readDouble();
        this.quantity = dataInput.readInt();
    }

    @Override
    public String toString() {
        return String.format("quantity:%d, revenue:%.2f", quantity, revenue);
    }


}
