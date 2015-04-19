import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class homework_1_part_3 {
	
	
	public static class Mapper_1 extends Mapper<LongWritable, Text, Text, Text>{
	    //Variable of type Text to store the name of the movie
		private Text movieName = new Text();  
	    //Store the movie genre given by the end-user 
	    String movieGenreList = "";
	    
	    //Method to check whether one of the Genre given by End user 
	    //exists in the GenreList from the file.
	    public boolean checkGenre(String userGenre, String[] inputGenreList){
	    	int loopVariable = 0;
	    	while(loopVariable < inputGenreList.length){
	    		if(userGenre.equalsIgnoreCase(inputGenreList[loopVariable]))
	    			return true;
	    		loopVariable++;
	    	}    		
	    	return false;
	    }
	    
	    @Override   
	    //Method to get the custom movie genre given by the end user at run time
	    protected  void setup(Context context) throws IOException, InterruptedException
	    { 
	        Configuration conf = context.getConfiguration();   
	        movieGenreList = conf.get("userGenre");   
	    }
	    
	    @Override
	    //Method to process the records and store only those having matching Genre as that of given by End user
	    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
		{
			//Read the single line at a time and store its value as String in line variable
	    	String line = values.toString();
			//Split the given line by its delimiters.
	    	String[] token = line.split("::");
			//Store the name of the movie
			movieName.set((token[1])); //Set movie name
			//Split Genres as they are stored as Genre1|Genre2|Genre3
			String[] genreList = token[2].split("\\|");			
			String[] userGenreList = movieGenreList.split(":");
			
			//An array, to check whether all Genres specified by 
			//End user are there in genreList from the input file.
			boolean[] isEqual = new boolean[userGenreList.length];
			int loopVariable = 0;
			//Assuming movie has all the genres given by the End user.
			while(loopVariable < isEqual.length){
				isEqual[loopVariable] = true;
				loopVariable++;
			}
			//Check if movie has genre's as that of given by user;
			if (genreList.length <= userGenreList.length){
				//Check userGenreList[0] with genreList[0]
				int anotherLoopVariable = 0;
				while(anotherLoopVariable < userGenreList.length){
					isEqual[anotherLoopVariable] = checkGenre(userGenreList[anotherLoopVariable], genreList);
					anotherLoopVariable++;
				}
			}
			loopVariable=0;
			boolean isWritable = true;
			//Now actually check whether movie qualifies or not.
			while(loopVariable < isEqual.length){
				if(!isEqual[loopVariable]){
					isWritable = false;
				}
				loopVariable++;				
			}
			if(isWritable){
				//Now write the movie name into file!
				context.write(movieName, new Text(""));
			}		
		}
	}
	
	public static void main(String[] args) throws Exception {
		 
	    Configuration conf = new Configuration();  
	    conf.set("userGenre",args[2]);
	    Job job = new Job(conf, "homework_1_part_3");
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setJarByClass(homework_1_part_3.class);
	    
	    job.setMapperClass(Mapper_1.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	    }
	}