package com.minhkakart.bigdata.cassandra;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * InputSplit for PlayerStats partition
 */
@SuppressWarnings("unused")
public class PlayerStatSplit extends InputSplit implements Writable {
    private long token;

    public PlayerStatSplit() {
        this.token = Long.MIN_VALUE;
    }

    public PlayerStatSplit(long token) {
        this.token = token;
    }

    @Override
    public long getLength() {
        return 1;
    }

    @Override
    public String[] getLocations() {
        return new String[0];
    }

    public long getToken() {
        return token;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(token);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        token = in.readLong();
    }
}
