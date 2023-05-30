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


public class Step3 {
    /**
     * Input:
     * 1) Key = <** r> Value = occurrences (Number of total 3-grams in all the corpus)
     * 2) Key = <w1 w2 w3> Value = r
     * 3) Key = <N 0 r> Value = occurrences (Sum of types) - N_r_0
     * 4) Key = <N 1 r> Value = occurrences (Sum of types) - N_r_1
     * 5) Key = <T 0 r> Value = occurrences (Sum of instances) - T_r_01
     * 6) Key = <T 1 r> Value = occurrences (Sum of instances) - T_r_10
     * <p>
     * Output:
     * 1) Key = <r, **> Value = <** N>
     * 2) Key = <r, **>  Value = <N 0 N_r_0>
     * 3) Key = <r, **>  Value = <N 1 N_r_1>
     * 4) Key = <r, **>  Value = <T 0 T_r_0>
     * 5) Key = <r, **>  Value = <T 1 T_r_1>
     * 6) Key = <r, w1 w2 w3> Value = <w1 w2 w3>
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            String[] words = keyValue[0].split(" ");
            if (words[0].equals("N") || words[0].equals("T"))
                context.write(new Text(String.format("%s %s", words[2], "**")), new Text(String.format("%s %s %s", words[0], words[1], keyValue[1])));
            else if (words[0].equals("**")) {
                context.write(new Text(String.format("%s %s", words[1], "**")), new Text(String.format("%s %s", "**", keyValue[1]))); // key - <r, **> value - <** occurrences>
            } else
                context.write(new Text(String.format("%s %s %s %s", keyValue[1], words[0], words[1], words[2])), new Text(String.format("%s %s %s", words[0], words[1], words[2]))); // <r, w1 w2 w3>  value - <w1 w2 w3>
        }
    }


    /**
     * Input:
     * 1) Key = <r **> Value = [** occurrences, N 0 occ, N 1 occ, T 0 occ, T 1 occ]
     * 2) Key = <r w1 w2 w3>  Value = [w1 w2 w3>]
     * <p>
     * Output:
     * Key = w1 w2 w3 Value = probability>
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        protected double N = 0.0;
        protected double N0 = 0.0;
        protected double N1 = 0.0;
        protected double T0 = 0.0;
        protected double T1 = 0.0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split(" ");

            if (words[1].equals("**")) {
                N = 0.0;
                N0 = 0.0;
                N1 = 0.0;
                T0 = 0.0;
                T1 = 0.0;
                while (values.iterator().hasNext()) {
                    String[] valueWords = values.iterator().next().toString().split(" ");
                    switch (valueWords[0]) {
                        case "**":
                            N = (double) Long.parseLong(valueWords[1]);
                            break;
                        case "N":
                            if (valueWords[1].equals("0"))
                                N0 = (double) Long.parseLong(valueWords[2]);
                            else
                                N1 = (double) Long.parseLong(valueWords[2]);
                            break;
                        case "T":
                            if (valueWords[1].equals("0"))
                                T0 = (double) Long.parseLong(valueWords[2]);
                            else
                                T1 = (double) Long.parseLong(valueWords[2]);
                            break;
                    }
                }
            } else { // <w1, w2, w3>
                Double probability;
                Text three_grams = values.iterator().next();
                if (N != 0 && (N0 + N1) != 0) {
                    probability = (T0 + T1) / (N * (N0 + N1));
                    context.write(three_grams, new Text(String.valueOf(probability)));
                } else
                    context.write(three_grams, new Text(String.valueOf(0)));
            }
        }
    }

    public static class Partition extends Partitioner<Text, Text> {

        @Override
        // Every r will come to the same partition
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs((key.toString().split(" ")[0]).hashCode() % numPartitions);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step3.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step3.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.addInputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_22/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("s3://bucket163897429777/output_step_22"));
//        FileOutputFormat.setOutputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_33"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_step_33"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}