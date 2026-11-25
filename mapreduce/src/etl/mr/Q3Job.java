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
 * Person B (Tushaar): Q3 Hourly Error Analysis.
 * Per (log_date, log_hour): error_request_count (status 400-599), total_request_count,
 * error_rate = errors / total, and distinct_error_hosts.
 *
 * map      (date, hour) -> Q3Partial(total 1, errors 0 or 1, {host} if it is an error)
 * combine  merges partials early: counts add up, host sets are unioned
 * reduce   merges again and writes one COPY-format line:
 *          log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
 *
 * Every valid request is mapped, not only errors, so an hour with traffic but no errors still
 * gets a row (with 0 errors, rate 0.0 and 0 hosts), as the contract requires.
 *
 * The input is one batch's parsed folder or the glob of all of them (.../parsed/batch_*).
 *
 * Usage: hadoop jar etl-mr.jar etl.mr.Q3Job <parsed input (folder or glob)> <output q3.tsv folder>
 */
public class Q3Job {

    public static class Q3Mapper extends Mapper<NullWritable, LogRecord, Text, Q3Partial> {
        private final Text key = new Text();
        private final Q3Partial one = new Q3Partial();

        @Override
        protected void map(NullWritable k, LogRecord r, Context ctx) throws IOException, InterruptedException {
            key.set(r.logDate + "\t" + r.logHour);
            one.clear();
            one.total = 1;
            if (r.isError()) {
                one.errors = 1;
                one.errorHosts.add(r.host);
            }
            ctx.write(key, one);
        }
    }

    public static class MergeCombiner extends Reducer<Text, Q3Partial, Text, Q3Partial> {
        private final Q3Partial sum = new Q3Partial();

        @Override
        protected void reduce(Text k, Iterable<Q3Partial> values, Context ctx) throws IOException, InterruptedException {
            sum.clear();
            for (Q3Partial v : values) sum.merge(v);
            ctx.write(k, sum);
        }
    }

    public static class Q3Reducer extends Reducer<Text, Q3Partial, Text, NullWritable> {
        private final Q3Partial sum = new Q3Partial();
        private final Text line = new Text();

        @Override
        protected void reduce(Text k, Iterable<Q3Partial> values, Context ctx) throws IOException, InterruptedException {
            sum.clear();
            for (Q3Partial v : values) sum.merge(v);
            String[] dateHour = k.toString().split("\t", 2);
            double rate = (double) sum.errors / (double) sum.total;
            line.set(CopyText.row(dateHour[0], dateHour[1], sum.errors, sum.total, rate, sum.errorHosts.size()));
            ctx.write(line, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }

    /** The job itself; returns 0 on success. RunJobs calls this to run several jobs in one JVM. */
    public static int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: Q3Job <parsed input> <output folder>");
            return 2;
        }
        Job job = Job.getInstance(Jobs.conf(), "etl q3");
        job.setJarByClass(Q3Job.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(Q3Mapper.class);
        job.setCombinerClass(MergeCombiner.class);
        job.setReducerClass(Q3Reducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Q3Partial.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (!job.waitForCompletion(false)) {
            System.err.println("Q3Job failed: " + job.getStatus().getFailureInfo());
            return 1;
        }
        return 0;
    }
}
