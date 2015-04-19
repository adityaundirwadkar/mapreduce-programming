import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.*;
import org.apache.hadoop.fs.*;


public class homework_1_part_1 {
	
	public static class Mapper_1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	    private final static DoubleWritable movieRating = new DoubleWritable();
	    private Text movieID = new Text();  
	    
	    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
		{
			 String line = values.toString();
			 line.trim();
			 String[] token = line.split("::");
			 //try{
				 //Get name of Movie
				 movieID.set(token[1]);
				 //Get Rating of the Movie
				 movieRating.set(Double.parseDouble(token[2]));
				 //Write it to a file.
				 context.write(movieID,movieRating);
			/* }
			 catch(Exception e){
				 System.out.println("A Blank entry has been found ignoring it...");
			 }*/
		}
	}
	
	public static class Reducer_1 extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
		DoubleWritable averageRating = new DoubleWritable();
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;        
			Double sum = 0.0;
	    	for (DoubleWritable val : values) {
	    		//Add all the ratings of every individual movie
	    		sum += val.get();
	    		//Keep track of number of movies encountered as well.
	    		count++;
	    		}
	    	//Now get the average rating for every movie. 
	    	averageRating.set(sum / (double)count);
	    	//Write it to the output file.
	    	context.write(key, averageRating);
	    	}
		}
	
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
        super(DoubleWritable.class, true);
        }
       
        @SuppressWarnings("rawtypes")
         
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            //Override the existing function to be able to comapare DoubleWritable values.
        	DoubleWritable rating1 = (DoubleWritable)w1;
            DoubleWritable rating2 = (DoubleWritable)w2;           
            return -1 * rating1.compareTo(rating2);
        }
    }
	
    public static class Mapper_2 extends Mapper<LongWritable, Text, DoubleWritable, Text>{
        private final static DoubleWritable movieRating = new DoubleWritable();
        private Text movieID = new Text(); 
       
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
             String line = values.toString();
             //Output of first mapper-reducer will be "MovieID\tAvgRating"
             String[] token = line.split("\t");
             //Get the Movie ID
             movieID.set(token[0]);
             //Get the rating of the movie.
             movieRating.set(Double.parseDouble(token[1]));
             //Output each (movieRating,MovieID) pair to KeyComparator class.
             context.write(movieRating,movieID);
        }
    }
   
    public static class Reducer_2 extends Reducer < DoubleWritable, Text, Text, DoubleWritable > {
    	//Keep a variable to count the total movies processed by reducer.
    	static int totalMoviesWritten = 0;
    	
    	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           for (Text movieID : values) {
                if(totalMoviesWritten < 10){
                	//Write output only if the movie is in top 10 records
                	context.write(movieID,key);
                	totalMoviesWritten++;
                	}
                }
           }
        }
    
    
	public static void main(String[] args) throws Exception {
		 
	    Configuration conf_1 = new Configuration();        
	    Job job_1 = new Job(conf_1, "homework_1_part_1");
	    
	    job_1.setOutputKeyClass(Text.class);
	    job_1.setOutputValueClass(DoubleWritable.class);
	    
	    job_1.setJarByClass(homework_1_part_1.class);
	    
	    job_1.setMapperClass(Mapper_1.class);
	    job_1.setCombinerClass(Reducer_1.class);
	    job_1.setReducerClass(Reducer_1.class);
	        
	    job_1.setInputFormatClass(TextInputFormat.class);
	    job_1.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job_1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job_1, new Path(args[1]));
	    
	    
	    //Only after completing first mapper-reducer move on to the next one.
	    if(job_1.waitForCompletion(true)){
	    	Path partitionFile = new Path(args[1] + "_partitions.lst");
	        
	        Configuration conf_2 = new Configuration();       
	        Job job_2 = new Job(conf_2, "homework_1_part_1_2");
	          
	        job_2.setOutputKeyClass(Text.class);
	        job_2.setOutputValueClass(DoubleWritable.class);
	        
	        job_2.setJarByClass(homework_1_part_1.class);
	        job_2.setMapperClass(Mapper_2.class);
	        job_2.setReducerClass(Reducer_2.class);
	       
	        job_2.setMapOutputKeyClass(DoubleWritable.class);
	        job_2.setMapOutputValueClass(Text.class);
	       
	        job_2.setInputFormatClass(TextInputFormat.class);
	        job_2.setOutputFormatClass(TextOutputFormat.class);
	           
	        FileInputFormat.addInputPath(job_2, new Path(args[1]));
	        FileOutputFormat.setOutputPath(job_2, new Path(args[1]+"_sorted_output"));
	       
	        TotalOrderPartitioner.setPartitionFile(job_2.getConfiguration(),partitionFile);
	        
	        job_2.setSortComparatorClass(KeyComparator.class);
	        job_2.waitForCompletion(true);
	        FileSystem.get(new Configuration()).delete(partitionFile, false);
	        }
	    }
	}