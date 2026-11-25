package etl.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Person B (Tushaar): a partial Q3 aggregate for one (log_date, log_hour).
 *
 * total       number of requests seen so far
 * errors      how many of them had status 400-599
 * errorHosts  the distinct hosts of those error requests
 *
 * Partials merge by adding the counts and taking the union of the host sets, so the same class
 * is the map output, the combiner output and the reducer input.
 */
public class Q3Partial implements Writable {
    public long total;
    public long errors;
    public final Set<String> errorHosts = new HashSet<>();

    public void clear() {
        total = 0;
        errors = 0;
        errorHosts.clear();
    }

    public void merge(Q3Partial other) {
        total += other.total;
        errors += other.errors;
        errorHosts.addAll(other.errorHosts);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(total);
        out.writeLong(errors);
        out.writeInt(errorHosts.size());
        for (String h : errorHosts) Text.writeString(out, h);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        total = in.readLong();
        errors = in.readLong();
        int n = in.readInt();
        for (int i = 0; i < n; i++) errorHosts.add(Text.readString(in));
    }
}
