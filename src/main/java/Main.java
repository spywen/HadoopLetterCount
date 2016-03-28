import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hadoop : Map/Reduce for LetterCount
 * This program enable us to count occurences of letters inside a text
 */
public class Main {

    //region LetterCount - COUNT

    /**
     * COUNT MAPPER : Map method to count letters
     */
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text letter = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //Select line
            String line = value.toString();

            //Define array of char corresponding to each letter found on the line
            char[] lett = line.toCharArray();

            //For each letter, count its presence (without considering match case by converting letter in UPPER CASE)
            //Except : whitespace
            for(char l : lett){
                if(l != ' '){
                    letter.set(String.valueOf(l).toUpperCase());
                    context.write(letter, one);
                }
            }

        }

    }

    /**
     * COUNT REDUCER : Reduce method to count letters
     */
    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;

            for (LongWritable val : values) {
                count += val.get();
            }

            context.write(key, new LongWritable(count));

        }
    }
    //endregion

    //region LetterCount - SORT

    /**
     * SORT MAPPER : Map method to sort letters
     */
    public static class sortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().trim().split("\\s+");

            context.write(new LongWritable(Long.parseLong(splits[1])), new Text(splits[0]));

        }

    }

    /**
     * SORT REDUCER : Reduce method to sort letters
     */
    public static class sortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(val, key);
            }

        }
    }

    /**
     * SORT COMPARATOR : COMPARATOR method to sort letters (compare long)
     */
    public static class sortComparator extends WritableComparator {

        protected sortComparator() {
            super(LongWritable.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {

            LongWritable k1 = (LongWritable) o1;
            LongWritable k2 = (LongWritable) o2;
            int cmp = k1.compareTo(k2);
            return -1 * cmp;

        }

    }
    //endregion

    /**
     * Main method :
     * 1) COUNT map/reduce
     * 2) SORT map/reduce
     * @param args :
     *             0 : input path which will contain the text to analyse (/user/student/input)
     *             1 : output path which will contain the result of the COUNT map/reduce (/user/student/outputCount)
     *             2 : output path which will contain the result of the SORT map/reduce (/user/student/outputSort)
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: LetterCount <in> <out> <out2>");
            System.exit(2);
        }

        //COUNT
        Job jobCount = new Job(conf, "LetterCount - COUNT");
        jobCount.setJarByClass(Main.class);

        jobCount.setMapperClass(Map.class);
        jobCount.setReducerClass(Reduce.class);

        jobCount.setMapOutputKeyClass(Text.class);
        jobCount.setMapOutputValueClass(LongWritable.class);

        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(LongWritable.class);

        jobCount.setInputFormatClass(TextInputFormat.class);
        jobCount.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(jobCount, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(jobCount, new Path(otherArgs[1]));

        jobCount.waitForCompletion(true);

        //SORT
        Job jobSort = new Job(conf, "LetterCount - SORT");
        jobSort.setJarByClass(Main.class);
        jobSort.setSortComparatorClass(sortComparator.class);

        jobSort.setMapperClass(sortMapper.class);
        jobSort.setReducerClass(sortReducer.class);

        jobSort.setMapOutputKeyClass(LongWritable.class);
        jobSort.setMapOutputValueClass(Text.class);

        jobSort.setOutputKeyClass(Text.class);
        jobSort.setOutputValueClass(LongWritable.class);

        jobSort.setInputFormatClass(TextInputFormat.class);
        jobSort.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(jobSort, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(jobSort, new Path(otherArgs[2]));

        jobSort.waitForCompletion(true);
    }

}