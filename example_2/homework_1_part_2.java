import java.io.IOException;
import java.util.*;        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class homework_1_part_2 {
        
 public static class Mapper_1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text movieGenre = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] token = line.split("::");
        //movieID::movieName::GenreList
        StringTokenizer genreList = new StringTokenizer(token[2],"|");
        //Emit (Genre,1) for every single Genre encountered.
        while (genreList.hasMoreTokens()) {
        	movieGenre.set(genreList.nextToken());
            context.write(movieGenre, one);
        }
    }
 } 
        
 public static class Reducer_1 extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
        	//For every single Genre add the total number of movies to get total number of movies for every single Genre.
            sum += val.get();
        }
        //Write it to the output file.
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();        
    Job job = new Job(conf, "homework_1_part_2");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJarByClass(homework_1_part_2.class);
    job.setMapperClass(Mapper_1.class);
    job.setCombinerClass(Reducer_1.class);
    job.setReducerClass(Reducer_1.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}