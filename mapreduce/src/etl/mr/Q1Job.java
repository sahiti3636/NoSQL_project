package etl.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Person A (Sahiti): Q1 Daily Traffic Summary.
 * Per (log_date, status_code): request_count and total_bytes.
 *
 * map      (date, status) -> (1, bytes)
 * combine  sums the pairs early, so far less data is shuffled
 * reduce   sums again and writes one COPY-format line: log_date, status_code, request_count, total_bytes
 *
 * The input is one batch's parsed folder, or a glob of all of them (.../parsed/batch_*) for the
 * whole run, so the same job serves process_batch() and finalize().
 *
 * Usage: hadoop jar etl-mr.jar etl.mr.Q1Job <parsed input (folder or glob)> <output q1.tsv folder>
 */
public class Q1Job {

    public static class Q1Mapper extends Mapper<NullWritable, LogRecord, Text, LongPair> {
        private final Text key = new Text();
        private final LongPair one = new LongPair();

        @Override
        protected void map(NullWritable k, LogRecord r, Context ctx) throws IOException, InterruptedException {
            key.set(r.logDate + "\t" + r.statusCode);
            one.set(1, r.bytes);
            ctx.write(key, one);
        }
    }

    public static class SumCombiner extends Reducer<Text, LongPair, Text, LongPair> {
        private final LongPair sum = new LongPair();

        @Override
        protected void reduce(Text k, Iterable<LongPair> values, Context ctx) throws IOException, InterruptedException {
            long count = 0, bytes = 0;
            for (LongPair v : values) {
                count += v.a;
                bytes += v.b;
            }
            sum.set(count, bytes);
            ctx.write(k, sum);
        }
    }

    public static class Q1Reducer extends Reducer<Text, LongPair, Text, NullWritable> {
        private final Text line = new Text();

        @Override
        protected void reduce(Text k, Iterable<LongPair> values, Context ctx) throws IOException, InterruptedException {
            long count = 0, bytes = 0;
            for (LongPair v : values) {
                count += v.a;
                bytes += v.b;
            }
            String[] dateStatus = k.toString().split("\t", 2);
            line.set(CopyText.row(dateStatus[0], dateStatus[1], count, bytes));
            ctx.write(line, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }

    /** The job itself; returns 0 on success. RunJobs calls this to run several jobs in one JVM. */
    public static int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: Q1Job <parsed input> <output folder>");
            return 2;
        }
        Job job = Job.getInstance(Jobs.conf(), "etl q1");
        job.setJarByClass(Q1Job.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(Q1Mapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(Q1Reducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (!job.waitForCompletion(false)) {
            System.err.println("Q1Job failed: " + job.getStatus().getFailureInfo());
            return 1;
        }
        return 0;
    }
}
