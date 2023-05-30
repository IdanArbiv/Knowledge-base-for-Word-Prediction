import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class Step1 {
    /**
     * Input:
     * Key = lineId (LongWritable)
     * Value = n-gram \t year \t occurrences \t pages \t books (Text)
     * Output:
     * 1) Number of occurrences in each part of the corpus - Key = <w1, w2, w3> Value = occurrences \t corpusPart(0/1)
     * 2) All three grams occurrences in the corpus (N) - Key = ** Value = occurrences
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, Integer> stopWords = new HashMap<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            String[] stopWordsArr = {"a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost",
                    "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst",
                    "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
                    "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before",
                    "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both",
                    "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry",
                    "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven",
                    "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere",
                    "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly",
                    "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt",
                    "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him",
                    "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is",
                    "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
                    "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself",
                    "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor",
                    "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other",
                    "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please",
                    "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several",
                    "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow",
                    "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system",
                    "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there",
                    "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
                    "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to",
                    "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under",
                    "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever",
                    "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein",
                    "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole",
                    "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your",
                    "yours", "yourself", "yourselves"};
            for (String word : stopWordsArr)
                stopWords.put(word, 0);
        }


        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String[] trigram = fields[0].split(" ");
            if (trigram.length > 2 && fields.length > 2) {
                String w1 = trigram[0];
                String w2 = trigram[1];
                String w3 = trigram[2];
                if (stopWords.containsKey(w1) || stopWords.containsKey(w2) || stopWords.containsKey(w3))
                    return;
                Text occurrences = new Text(fields[2]);
                int corpus_half = (int) Math.round(Math.random()); // 0 OR 1
                context.write(new Text("**"), occurrences);
                context.write(new Text(String.format("%s %s %s", w1, w2, w3)), new Text(String.format("%s %s", occurrences, corpus_half)));
            }
        }

    }

    /**
     * Input:
     * 1) Number of occurrences in each part of the corpus - Key = <w1, w2, w3> Value = occurrences \t corpusPart(0 OR 1)
     * 2) All three grams occurrences in the corpus (N) - Key = ** Value = occurrences
     * Output:
     * 1) Key = <w1, w2, w3> Value = r \t R0 \t R1
     * r = Number of occurrences in all the corpus
     * R0 = Number Of occurrences in the first half
     * R1 = Number Of occurrences in the second half
     * 2) Number of 3-grams in all the corpus  - Key = **, Value = totalNumberOf3-grams
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("**")) {
                long totalNumberOf3Grams = 0;
                for (Text value : Values) {
                    long occurrencesForOne3gram = Long.parseLong(value.toString());
                    totalNumberOf3Grams += occurrencesForOne3gram;
                }
                context.write(key, new Text(String.format("%d", totalNumberOf3Grams)));
            } else { // Key = <w1,w2,w3>
                long r = 0, R0 = 0, R1 = 0;
                for (Text value : Values) {
                    String[] valueSplit = value.toString().split(" "); // [occurrences(number), 0 OR 1] 0 - first part of the corpus 1 - second part of the corpus
                    if (valueSplit[1].equals("0")) // First part of the corpus
                        R0 += Long.parseLong(valueSplit[0]);
                    else // Second part of the corpus
                        R1 += Long.parseLong(valueSplit[0]);
                    r += Long.parseLong(valueSplit[0]); // In any case we want to count the number of occurrences in all the corpus for the 3-gram
                }
                context.write(key, new Text(String.format("%s\t%s\t%s", r, R0, R1)));
            }
        }
    }


    /**
     * Input:
     * 1) Number of occurrences in each part of the corpus - Key = <w1, w2, w3> Value = occurrences \t corpusPart(0 OR 1)
     * 2) All three grams occurrences in the corpus (N) - Key = ** Value = occurrences
     * Output:
     * 1) Key = <w1, w2, w3> Value = R0 \t 0
     * 2) Key = <w1, w2, w3> Value = R1 \t 1
     * R0 = Number Of occurrences in the first half
     * R1 = Number Of occurrences in the second half
     * 3) Number of 3-grams in all the corpus  - Key = **, Value = totalNumberOf3-grams
     */
    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("**")) {
                long totalNumberOf3Grams = 0;
                for (Text value : Values) {
                    long occurrencesForOne3gram = Long.parseLong(value.toString());
                    totalNumberOf3Grams += occurrencesForOne3gram;
                }
                context.write(key, new Text(String.format("%d", totalNumberOf3Grams)));
            } else {
                long R0 = 0, R1 = 0;
                for (Text value : Values) {
                    String[] valueSplit = value.toString().split(" "); // [occurrences(number), 0 OR 1]
                    if (valueSplit[1].equals("0")) // First part of the corpus
                        R0 += Integer.parseInt(valueSplit[0]);
                    else // // Second part of the corpus
                        R1 += Integer.parseInt(valueSplit[0]);
                }
                context.write(key, new Text(String.format("%s %s", R0, 0)));
                context.write(key, new Text(String.format("%s %s", R1, 1)));
            }
        }
    }

    public static class Partition extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step1.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step1.Partition.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setCombinerClass(Step1.Combiner.class);
//        job.setInputFormatClass(TextInputFormat.class); // Example File
//        TextInputFormat.addInputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/3_grams.txt"));
//        TextInputFormat.addInputPath(job, new Path("s3://bucket163897429777/3_grams.txt"));
        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));
//        FileOutputFormat.setOutputPath(job, new Path("/home/spl211/IdeaProjects/MapReduceProject/output_step_11"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_step_11"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
