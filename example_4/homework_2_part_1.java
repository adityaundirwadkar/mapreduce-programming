package assignment_2;

import java.io.*;
import java.util.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class homework_2_part_1 {
	
	public static class Mapper_1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		private Text movieID = new Text();
        private DoubleWritable movieRating = new DoubleWritable();
        public void map(LongWritable key, Text values, Context context)throws IOException, InterruptedException {
        	//Use try catch to avoid any faulty inputs
	    	try{
	    		String line = values.toString();
				line.trim();
				String[] token = line.split("::");
				movieID.set(new Text(token[1]));
				movieRating.set(Double.parseDouble(token[2]));
				context.write(movieID,movieRating);
	    	}
	    	catch(Exception e){
	    		System.out.print("Found an incomplete entry in Ratings file...");
	    	}       	
        }
	}
        
   public static class Reducer_1 extends Reducer <Text, DoubleWritable, Text, Text> {
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
       	context.write(new Text(key+"::"+averageRating.toString()), new Text());
       	}
   	}
   
   public static class KeyComparator extends WritableComparator {
       protected KeyComparator() {
       super(DoubleWritable.class, true);
       }
      
       @SuppressWarnings("rawtypes")
        
       @Override
       public int compare(WritableComparable w1, WritableComparable w2) {
           //Override the existing function to be able to compare DoubleWritable values.
       	DoubleWritable rating1 = (DoubleWritable)w1;
           DoubleWritable rating2 = (DoubleWritable)w2;           
           return -1 * rating1.compareTo(rating2);
       }
   }

	public static class Mapper_2 extends Mapper<LongWritable, Text, DoubleWritable , Text>{

		private static Map<String, String> hashMap = new HashMap<String, String>();
		File file;
		public void setup(Context context) {
			try{
				Configuration conf = context.getConfiguration();
				Path path[] = DistributedCache.getLocalCacheFiles(conf);
                file = new File(path[0].toString());
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                String line="";
                while((line=bufferedReader.readLine())!= null)
                {
                    String[] token = line.split("::", 2);
                    hashMap.put(token[0], token[1]);
                }
                bufferedReader.close();
			}
			catch(Exception e){
				System.out.println("Exception in second mapper!");
			}
		}
		
		public void map(LongWritable key, Text values, Context context)throws IOException, InterruptedException {
			Text movieInfo=new Text();
            DoubleWritable rating = new DoubleWritable();
            String line = values.toString();
            String[] token = line.split("::",2);
            boolean answer=false;
            boolean inMemory=context.getConfiguration().getBoolean("ratingsFileInMemory", true);
            if(inMemory)
            {
            	movieInfo.set(new Text(token[0]+"\t"+token[1].replaceAll("::", "\t")));
                try{
                	rating.set(Double.parseDouble(hashMap.get(token[0])));
                }
                catch(NullPointerException npe){
                	rating.set(1.1);
                }
                if(token[1].contains("Action")){ 
                	answer=true;
                }
            }
            else{
            	try{
            		movieInfo.set(new Text(token[0]+"\t"+hashMap.get(token[0]).replaceAll("::", "\t")));
            	}
            	catch(NullPointerException npe){
            		movieInfo.set("Couldnot find a movie!");
            	}
            	try{
            		rating.set(Double.parseDouble(token[1]));
            	}catch(NullPointerException npe){
            		rating.set(0.0);
            	}
            	try{
            		if(hashMap.get(token[0]).contains("Action")){
            			answer=true;
            		}
                }
            	catch(NullPointerException npe){}
            }           
            if(answer){
            	 context.write(rating,movieInfo);
             }
		}
	}
	
	public static class Reducer_2 extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		DoubleWritable averageRating = new DoubleWritable();
		static int counter =0;
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text text : values) {
				if(counter<10){
					context.write(text , key);
					counter++;
				}				
			}
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf_1 = new Configuration();
        Job job_1 = new Job(conf_1, "homework_2_part_1");
        
        Path ratingsInput = new Path(args[0]+"/ratings.dat");
		Path moviesInput = new Path(args[0]+"/movies.dat");
		Path intermediateOutput = new Path(args[1]+"_intermediate");

		Path outputFile = new Path(args[1]);
 

		job_1.setOutputKeyClass(Text.class);
		job_1.setOutputValueClass(DoubleWritable.class);
		job_1.setJarByClass(homework_2_part_1.class);

		job_1.setMapperClass(Mapper_1.class);
		job_1.setReducerClass(Reducer_1.class);

		job_1.setInputFormatClass(TextInputFormat.class);
		job_1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job_1, ratingsInput);
        FileOutputFormat.setOutputPath(job_1, intermediateOutput);

        if (job_1.waitForCompletion(true))
        {
            Configuration conf_2 = new Configuration();
            FileSystem fs = FileSystem.get(conf_2);
            Path intermediateInput;
            Long lengthIntermediate = fs.getFileStatus(new Path(intermediateOutput+"/part-r-00000")).getLen();
            Long lengthMovies = fs.getFileStatus(moviesInput).getLen();
            if (lengthIntermediate < lengthMovies) {
                DistributedCache.addCacheFile(new Path(intermediateOutput+"/part-r-00000").toUri(), conf_2);
                intermediateInput = moviesInput;
                conf_2.setBoolean("ratingsFileInMemory", true);
            }
            else
            {
                DistributedCache.addCacheFile(moviesInput.toUri(), conf_2);
                intermediateInput = intermediateOutput;
                conf_2.setBoolean("ratingsFileInMemory", false);
            }
            DistributedCache.addCacheFile(new Path(intermediateOutput+"/part-r-00000").toUri(), conf_2);
            Job job_2 = new Job(conf_2, "homework_2_part_1");
           
            job_2.setJarByClass(homework_2_part_1.class);
            job_2.setMapperClass(Mapper_2.class);
            job_2.setReducerClass(Reducer_2.class);
            
            job_2.setMapOutputKeyClass(DoubleWritable.class);
            job_2.setMapOutputValueClass(Text.class);
            job_2.setInputFormatClass(TextInputFormat.class);
            
            job_2.setOutputFormatClass(TextOutputFormat.class);
            job_2.setOutputKeyClass(Text.class);
            job_2.setOutputValueClass(DoubleWritable.class);
            
            job_2.setSortComparatorClass(KeyComparator.class);
            
            FileInputFormat.addInputPath(job_2, intermediateInput);
            FileOutputFormat.setOutputPath(job_2, outputFile);

            job_2.waitForCompletion(true);
        }
    }
}