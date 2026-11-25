package etl.mr;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Person B (Tushaar): Q2 Top Requested Resources.
 * The 20 resource paths with the most requests: request_count, total_bytes, distinct_host_count.
 *
 * Two chained jobs, started from one main() so the batch pays for one JVM:
 *
 *   Job 1 "top"    map      path -> (1, bytes)
 *                  combine  sums the pairs early
 *                  reduce   (one reducer) sums per path and keeps the best 20 in a bounded heap.
 *                           Order: request_count desc, then resource_path in UTF-8 byte order
 *                           (Text compares raw bytes, like PostgreSQL's C collation; Java's
 *                           String.compareTo compares UTF-16 units and would differ above U+E000).
 *                           Writes the 20 winners (path, count, bytes) to a scratch SequenceFile.
 *
 *   Job 2 "hosts"  map      only rows whose path is one of the 20 winners: path -> host
 *                  combine  drops repeated hosts within one map task
 *                  reduce   (one reducer) counts distinct hosts per winner, then writes the 20
 *                           COPY-format lines in rank order:
 *                           resource_path, request_count, total_bytes, distinct_host_count
 *
 * Distinct hosts are only counted for the 20 winners, so the expensive part stays small.
 * The winner list reaches the job 2 mappers through the job Configuration (Base64 of the UTF-8
 * bytes, so a path with a comma, tab or any other byte survives).
 *
 * The input is one batch's parsed folder or the glob of all of them (.../parsed/batch_*), so the
 * same job serves process_batch() and finalize().
 *
 * Usage: hadoop jar etl-mr.jar etl.mr.Q2Job <parsed input (folder or glob)> <output q2.tsv folder>
 */
public class Q2Job {

    static final int TOP_N = 20;
    static final String WINNERS = "etl.q2.winners";          // conf key: Base64 paths, comma-separated

    /** Rank order of Contract 2: request_count desc, then resource_path in byte order. */
    static final Comparator<Winner> RANK = (x, y) -> {
        int c = Long.compare(y.count, x.count);
        return c != 0 ? c : x.path.compareTo(y.path);           // Text.compareTo = unsigned byte order
    };

    /** One candidate in the top 20. */
    static final class Winner {
        final Text path;
        final long count;
        final long bytes;

        Winner(Text path, long count, long bytes) {
            this.path = path;
            this.count = count;
            this.bytes = bytes;
        }
    }

    // ------------------------------------------------------------------ job 1: counts + top 20
    public static class CountMapper extends Mapper<NullWritable, LogRecord, Text, LongPair> {
        private final Text key = new Text();
        private final LongPair one = new LongPair();

        @Override
        protected void map(NullWritable k, LogRecord r, Context ctx) throws IOException, InterruptedException {
            key.set(r.resourcePath);
            one.set(1, r.bytes);
            ctx.write(key, one);
        }
    }

    public static class TopReducer extends Reducer<Text, LongPair, Text, LongPair> {
        // Worst candidate at the head, so the heap can drop it once it holds more than 20.
        private final PriorityQueue<Winner> top = new PriorityQueue<>(TOP_N + 1, RANK.reversed());

        @Override
        protected void reduce(Text path, Iterable<LongPair> values, Context ctx) {
            long count = 0, bytes = 0;
            for (LongPair v : values) {
                count += v.a;
                bytes += v.b;
            }
            top.add(new Winner(new Text(path), count, bytes));
            if (top.size() > TOP_N) top.poll();
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            List<Winner> best = new ArrayList<>(top);
            best.sort(RANK);
            for (Winner w : best) ctx.write(w.path, new LongPair(w.count, w.bytes));
        }
    }

    // ------------------------------------------------------------------ job 2: distinct hosts
    public static class HostMapper extends Mapper<NullWritable, LogRecord, Text, Text> {
        private final Set<String> winners = new HashSet<>();
        private final Text path = new Text();
        private final Text host = new Text();

        @Override
        protected void setup(Context ctx) {
            winners.addAll(decode(ctx.getConfiguration().get(WINNERS, "")));
        }

        @Override
        protected void map(NullWritable k, LogRecord r, Context ctx) throws IOException, InterruptedException {
            if (!winners.contains(r.resourcePath)) return;
            path.set(r.resourcePath);
            host.set(r.host);
            ctx.write(path, host);
        }
    }

    /** Emits each host once per path (safe as a combiner: the reducer de-duplicates again). */
    public static class DistinctHostCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text path, Iterable<Text> hosts, Context ctx) throws IOException, InterruptedException {
            Set<String> seen = new HashSet<>();
            for (Text h : hosts) {
                String s = h.toString();
                if (seen.add(s)) ctx.write(path, new Text(s));
            }
        }
    }

    public static class HostReducer extends Reducer<Text, Text, Text, NullWritable> {
        private final Map<String, Long> distinct = new HashMap<>();

        @Override
        protected void reduce(Text path, Iterable<Text> hosts, Context ctx) {
            Set<String> seen = new HashSet<>();
            for (Text h : hosts) seen.add(h.toString());
            distinct.put(path.toString(), (long) seen.size());
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            // Rank order comes from the winner list written by job 1 (already sorted).
            Configuration conf = ctx.getConfiguration();
            List<String> order = decode(conf.get(WINNERS, ""));
            String[] counts = conf.getStrings("etl.q2.counts", new String[0]);
            String[] bytes = conf.getStrings("etl.q2.bytes", new String[0]);
            Text line = new Text();
            for (int i = 0; i < order.size(); i++) {
                String p = order.get(i);
                Long hosts = distinct.get(p);
                if (hosts == null) {
                    throw new IOException("Q2: no hosts found for winner " + p);
                }
                line.set(CopyText.row(p, counts[i], bytes[i], hosts));
                ctx.write(line, NullWritable.get());
            }
        }
    }

    // ------------------------------------------------------------------ helpers
    static String encode(List<String> paths) {
        StringBuilder b = new StringBuilder();
        for (String p : paths) {
            if (b.length() > 0) b.append(',');
            b.append(Base64.getEncoder().encodeToString(p.getBytes(StandardCharsets.UTF_8)));
        }
        return b.toString();
    }

    static List<String> decode(String s) {
        if (s == null || s.isEmpty()) return Collections.emptyList();
        List<String> out = new ArrayList<>();
        for (String part : s.split(",", -1)) {
            out.add(new String(Base64.getDecoder().decode(part), StandardCharsets.UTF_8));
        }
        return out;
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }

    /** The job itself; returns 0 on success. RunJobs calls this to run several jobs in one JVM. */
    public static int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: Q2Job <parsed input> <output folder>");
            return 2;
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path scratch = new Path(output.getParent(), "_q2_top_" + output.getName());

        // Job 1: request count and bytes per path, keep the top 20.
        Configuration conf1 = Jobs.conf();
        FileSystem fs = FileSystem.getLocal(conf1);
        fs.delete(scratch, true);
        Job top = Job.getInstance(conf1, "etl q2 top");
        top.setJarByClass(Q2Job.class);
        top.setInputFormatClass(SequenceFileInputFormat.class);
        top.setMapperClass(CountMapper.class);
        top.setCombinerClass(Q1Job.SumCombiner.class);          // same (count, bytes) sum as Q1
        top.setReducerClass(TopReducer.class);
        top.setNumReduceTasks(1);                               // one global top 20
        top.setMapOutputKeyClass(Text.class);
        top.setMapOutputValueClass(LongPair.class);
        top.setOutputKeyClass(Text.class);
        top.setOutputValueClass(LongPair.class);
        top.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(top, input);
        FileOutputFormat.setOutputPath(top, scratch);
        if (!top.waitForCompletion(false)) {
            System.err.println("Q2Job (top) failed: " + top.getStatus().getFailureInfo());
            return 1;
        }

        // Read the (at most 20) winners back, in rank order.
        List<Winner> winners = new ArrayList<>();
        for (org.apache.hadoop.fs.FileStatus f : fs.listStatus(scratch)) {
            String name = f.getPath().getName();
            if (!name.startsWith("part-")) continue;
            try (SequenceFile.Reader r = new SequenceFile.Reader(conf1, SequenceFile.Reader.file(f.getPath()))) {
                Text path = new Text();
                LongPair v = new LongPair();
                while (r.next(path, v)) winners.add(new Winner(new Text(path), v.a, v.b));
            }
        }
        winners.sort(RANK);
        fs.delete(scratch, true);

        List<String> paths = new ArrayList<>();
        List<String> counts = new ArrayList<>();
        List<String> bytes = new ArrayList<>();
        for (Winner w : winners) {
            paths.add(w.path.toString());
            counts.add(Long.toString(w.count));
            bytes.add(Long.toString(w.bytes));
        }

        // Job 2: distinct hosts of the winners, and the final q2.tsv lines.
        Configuration conf2 = Jobs.conf();
        conf2.set(WINNERS, encode(paths));
        conf2.setStrings("etl.q2.counts", counts.toArray(new String[0]));
        conf2.setStrings("etl.q2.bytes", bytes.toArray(new String[0]));
        Job hosts = Job.getInstance(conf2, "etl q2 hosts");
        hosts.setJarByClass(Q2Job.class);
        hosts.setInputFormatClass(SequenceFileInputFormat.class);
        hosts.setMapperClass(HostMapper.class);
        hosts.setCombinerClass(DistinctHostCombiner.class);
        hosts.setReducerClass(HostReducer.class);
        hosts.setNumReduceTasks(1);                             // writes the 20 lines in rank order
        hosts.setMapOutputKeyClass(Text.class);
        hosts.setMapOutputValueClass(Text.class);
        hosts.setOutputKeyClass(Text.class);
        hosts.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(hosts, input);
        FileOutputFormat.setOutputPath(hosts, output);
        if (!hosts.waitForCompletion(false)) {
            System.err.println("Q2Job (hosts) failed: " + hosts.getStatus().getFailureInfo());
            return 1;
        }
        return 0;
    }
}
