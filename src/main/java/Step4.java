import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step4 {

    /**
     * Input:
     * <w1 w2 w3, probability>
     * <p>
     * Output:
     * <w1 w2 w3 probability, "">
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            context.write(new Text(String.format("%s %s", keyValue[0], keyValue[1])), new Text(""));
        }
    }

    /**
     * Input:
     * <w1 w2 w3 probability, "">
     * <p>
     * Output:
     * <w1 w2 w3 probability, "">
     */

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyValue = key.toString().split(" "); // w1 w2 w3 probability
            context.write(new Text((String.format("%s %s %s", keyValue[0], keyValue[1], keyValue[2]))), new Text(keyValue[3]));
        }

    }

    public static class Partition extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    private static class Comparison extends WritableComparator {
        protected Comparison() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) { // key: w1 w2 w3 probability
            String[] words1 = key1.toString().split(" ");
            String[] words2 = key2.toString().split(" ");
            if (words1[0].equals(words2[0]) && words1[1].equals(words2[1])) {
                if (Double.parseDouble(words1[3]) >= (Double.parseDouble(words2[3]))) {
                    return -1;
                } else
                    return 1;
            }
            return String.format("%s %s", words1[0], words1[1]).compareTo(String.format("%s %s", words2[0], words2[1]));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setSortComparatorClass(Step4.Comparison.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Step4.Partition.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.addInputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_33/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("s3://bucket163897429777/output_step_33"));
//        FileOutputFormat.setOutputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_44"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_step_44"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}