package etl.mr;

import org.apache.hadoop.conf.Configuration;

/**
 * The Configuration every pipeline job starts from.
 *
 * Job.waitForCompletion(false) checks whether the job has finished only every
 * mapreduce.client.completion.pollinterval ms, 5,000 by default. A batch runs five jobs (parse,
 * Q1, two for Q2, Q3), so up to 25 s per batch went into waiting for a job that had already
 * finished. 100 ms keeps the check cheap and removes that idle time. It changes no result.
 */
public final class Jobs {
    private Jobs() {}

    public static Configuration conf() {
        Configuration conf = new Configuration();
        conf.setInt("mapreduce.client.completion.pollinterval", 100);
        return conf;
    }
}
