import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step2 {
    /**
     * Input:
     * 1) Key = lineId Value = <w1 w2 w3 \t r \t R1 \t R2>
     * 2) Key - lineId Value = <** \t occurrences>
     * <p>
     * Output:
     * 1) Key = <** **> Value = occurrences (Number of total 3-grams in all the corpus)
     * 2) Key = <w1 w2 w3> Value = r
     * 3) Key = <N 0 R0> Value = 1 (types)
     * 4) Key = <T 1 R0> Value = R1 (instances)
     * 5) Key = <N 1 R1> Value = 1 (types)
     * 6) Key = <T 0 R1> Value = R0 (instances)
     * 7) Key = <**, r> Value = r (For each <w1,w2,w3> that in the input)
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t"); // [w1 w2 w3, r, R1, R2] OR [**, occurrences]
            String[] words = keyValue[0].split(" "); // [w1, w2, w3] OR [**]
            if (words.length < 3)  // Value = <** occurrences>
                context.write(new Text(String.format("%s %s", "**", "**")), new Text(keyValue[1])); // 1) Key = <* *> Value = occurrences
            else { // Value = <w1, w2, w3 \t r \t R1 \t R2>
                String w1 = words[0], w2 = words[1], w3 = words[2];
                String r = keyValue[1], R0 = keyValue[2], R1 = keyValue[3];
                context.write(new Text(String.format("%s %s %s", w1, w2, w3)), new Text(r)); // 2) Key = <w1 w2 w3> Value = r
                if (Long.parseLong(R0) > 0) {
                    context.write(new Text(String.format("%s %s %s", "N", 0, R0)), new Text("1")); // 3) Key = <N 0 r> Value = 1 (types)
                    context.write(new Text(String.format("%s %s %s", "T", 1, R0)), new Text(R1)); // 4) Key = <T 1 R0> Value = R1 (instances)
                }
                if (Long.parseLong(R1) > 0) {
                    context.write(new Text(String.format("%s %s %s", "N", 1, R1)), new Text("1")); // 5) Key = <N 1 r> Value = 1 (types)
                    context.write(new Text(String.format("%s %s %s", "T", 0, R1)), new Text(R0)); // 6) Key = <T 0 R1> Value = R0 (instances)
                }
                context.write(new Text(String.format("%s %s", "**", r)), new Text(r)); // 7) Key = <** r> Value = r
            }
        }
    }

    /**
     * Input:
     * 1) Key = <** **> Value = occurrences (Number of total 3-grams in all the corpus)
     * 2) Key = <w1 w2 w3> Value = r
     * 3) Key = <N 0 R0> Value = 1 (types)
     * 4) Key = <T 1 R0> Value = R1 (instances)
     * 5) Key = <N 1 R1> Value = 1 (types)
     * 6) Key = <T 0 R1> Value = R0 (instances)
     * 7) Key = <**, r> Value = r (For each <w1,w2,w3> that in the input)
     * <p>
     * Output:
     * 1) Key = <** r> Value = occurrences (Number of total 3-grams in all the corpus)
     * 2) Key = <w1 w2 w3> Value = r
     * 3) Key = <N 0 r> Value = occurrences (Sum of types) - N_r_0
     * 4) Key = <N 1 r> Value = occurrences (Sum of types) - N_r_1
     * 5) Key = <T 0 r> Value = occurrences (Sum of instances) - T_r_01
     * 6) Key = <T 1 r> Value = occurrences (Sum of instances) - T_r_10
     * <p>
     * N_r_0 = is the number of n-gram types occurring r times in the first part of the corpus.
     * T_r_01 = is the total number the n-grams of the first part (of N_r_0) appear the second part of the corpus (instances).
     * N_r_1 = is the number of n-gram types occurring r times in the second part of the corpus.
     * Tr_10 = is the total number the n-grams of the second part (of N_r_1) appear in the first part of the corpus (instance).
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        protected long N = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // [**] OR [w1, w2, w3] OR [N, 0, r] OR [N, 1, r] OR [T, 0, r]  OR [T, 1, r]
            String[] words = key.toString().split(" ");
            String lastWord = "";
            if (words[0].equals("**")) {
                for (Text N_OR_r : values) {
                    if (words[1].equals("**")) {
                        N = Long.parseLong(N_OR_r.toString());
                    } else {
                        String r = N_OR_r.toString();
                        if (!lastWord.equals(r))
                            context.write(new Text(String.format("%s %s", "**", r)), new Text(String.format("%d", N)));
                        lastWord = r;
                    }
                }
            } else if (words[0].equals("N") || words[0].equals("T")) {
                long sum = 0;
                for (Text value : values) {
                    try {
                        sum += Long.parseLong(value.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                context.write(key, new Text(String.format("%d", sum)));
            } else // words = [w1, w2, w3]
                context.write(key, new Text(values.iterator().next()));
        }
    }


    /**
     * Input:
     * 1) Key = <** **> Value = occurrences (Number of total 3-grams in all the corpus)
     * 2) Key = <w1 w2 w3> Value = r
     * 3) Key = <N 0 R0> Value = 1 (types)
     * 4) Key = <T 1 R0> Value = R1 (instances)
     * 5) Key = <N 1 R1> Value = 1 (types)
     * 6) Key = <T 0 R1> Value = R0 (instances)
     * 7) Key = <**, r> Value = r (For each <w1,w2,w3> that in the input)
     * <p>
     * Output:
     * 1) Key = <** **> Value = occurrences (Number of total 3-grams in all the corpus)
     * 2) Key = <w1 w2 w3> Value = r
     * 3) Key = <N 0 r> Value = occurrences (Sum of types) - N_r_0
     * 4) Key = <N 1 r> Value = occurrences (Sum of types) - N_r_1
     * 5) Key = <T 0 r> Value = occurrences (Sum of instances) - T_r_01
     * 6) Key = <T 1 r> Value = occurrences (Sum of instances) - T_r_10
     * 7) Key = <**, r> Value = r (For each <w1,w2,w3> that in the input)
     * <p>
     * N_r_0 = is the number of n-gram types occurring r times in the first part of the corpus.
     * T_r_01 = is the total number the n-grams of the first part (of N_r_0) appear the second part of the corpus (instances).
     * N_r_1 = is the number of n-gram types occurring r times in the second part of the corpus.
     * Tr_10 = is the total number the n-grams of the second part (of N_r_1) appear in the first part of the corpus (instance).
     */
    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // [**] OR [w1, w2, w3] OR [N, 0, r] OR [N, 1, r] OR [T, 0, r]  OR [T, 1, r]
            String[] words = key.toString().split(" ");
            if (words[0].equals("N") || words[0].equals("T")) {
                long sum = 0;
                for (Text value : values) {
                    try {
                        sum += Long.parseLong(value.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                context.write(key, new Text(String.format("%d", sum)));
            } else
                for (Text value : values)
                    context.write(key, value);
        }
    }

    public static class Partition extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs((key.toString().split(" ")[0]).hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step2.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step2.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setCombinerClass(Step2.Combiner.class);
//        FileInputFormat.addInputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_11/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("s3://bucket163897429777/output_step_11"));
//        FileOutputFormat.setOutputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_22"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_step_22"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}