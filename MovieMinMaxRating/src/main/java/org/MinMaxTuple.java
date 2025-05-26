package org;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxTuple implements Writable {
    private float min;
    private float max;

    public MinMaxTuple(){}

    public MinMaxTuple(float min, float max){
        this.min = min;
        this.max = max;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(min);
        dataOutput.writeFloat(max);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        min = dataInput.readFloat();
        max = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return min + "\t" + max;
    }
}
