package com.guyi.hadoop.pojo;

import com.guyi.hadoop.mapreduce.SecondarySortApp;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 功能 :
 *
 * @author : 谷燚
 * @version : 1.0
 * @date : 2021-08-04 09:49
 * @package : com.guyi.hadoop.pojo
 * @email : 853779011@qq.com
 */
public class IntPair implements WritableComparable<IntPair> {
    private int first = 0;
    private int second = 0;

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public void set(int left, int right){
        first = left;
        second = right;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        return first + "".hashCode() + second + "".hashCode();
    }

    @Override
    public boolean equals(Object right) {
        if (right instanceof IntPair){
            IntPair r = (IntPair) right;
            return r.first == first && r.second == second;
        } else {
            return  false;
        }
    }

    // 对key排序，调用的是compareTo方法
    public int compareTo(IntPair o) {
        if (first != o.first){
            return first - o.first;
        }else if(second != o.second){
            return second - o.second;
        }else {
            return 0;
        }
    }
}
