package etl.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** Two longs, e.g. (request count, total bytes). Used as a map/combine value. */
public class LongPair implements Writable {
    public long a;
    public long b;

    public LongPair() {}

    public LongPair(long a, long b) {
        this.a = a;
        this.b = b;
    }

    public void set(long a, long b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(a);
        out.writeLong(b);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        a = in.readLong();
        b = in.readLong();
    }
}
