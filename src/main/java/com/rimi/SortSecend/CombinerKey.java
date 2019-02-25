package com.rimi.SortSecend;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CombinerKey implements WritableComparable {
    private Integer year;
    private Integer temp;

    public Integer getTemp() {
        return temp;
    }

    public Integer getYear() {
        return year;
    }

    public void setTemp(Integer temp) {
        this.temp = temp;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    @Override
    public int compareTo(Object o) {
        int year0 = ((CombinerKey)o).getYear();
        int temp0 = ((CombinerKey)o).getTemp();
        return year0 - this.year == 0 ? -(temp0 - this.temp) : year0 - this.year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.temp);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int year = in.readInt();
        this.temp = in.readInt();

    }
}
