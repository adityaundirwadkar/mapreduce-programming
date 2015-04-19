package assignment_2;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.*;


public class homework_2_part_2 {
	
	//Class to arrange Users from users file.
	public static class Mapper_1 extends Mapper<LongWritable, Text, TextPair, Text>{
	    private TextPair userID = new TextPair();
	    private Text userGender = new Text();  
	    
	    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
	    	//Use try catch to avoid any faulty inputs
	    	try{
	    		String line = values.toString();
				line.trim();
				String[] token = line.split("::");
				userID.set(new Text(token[0]), new Text("0"));
				userGender.set(token[1]);
				//Write <(userID,0),(userGender)>
				context.write(userID,userGender);
	    	}
	    	catch(Exception e){
	    		System.out.print("Found an incomplete entry in Users file...");
	    	}
		}
	}
	
	//Map class for ratings
	public static class Mapper_2 extends Mapper<LongWritable, Text, TextPair, Text>{
		private TextPair userID = new TextPair();
	    private Text movieInfo = new Text();
	    
	    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
	    	//Use try catch to avoid any faulty inputs
	    	try{
	    		String line = values.toString();
				line.trim();
				String[] token = line.split("::");
				userID.set(new Text(token[0]), new Text("1"));
				movieInfo.set(token[1]+"::"+token[2]); //Append movieID and its Rating
				//Write <(userID,1),(movieID::rating)>
				context.write(userID,movieInfo); //movieInfo = movieID::rating
	    	}
	    	catch(Exception e){
	    		System.out.print("Found an incomplete entry in Ratings file...");
	    	}
		}
	}
	
	public static class Reducer_1 extends Reducer <TextPair, Text, Text, Text> {
		public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			@SuppressWarnings("unused")
			Text userID=new Text(key.getFirst());
			Iterator<Text> textIterator= values.iterator();
			Text userGender=new Text(textIterator.next());

			if(userGender.toString().equalsIgnoreCase("M")){
				while(textIterator.hasNext()) {
					Text remainingString=textIterator.next();
					//context.write(userID, new Text(userGender.toString()+"::"+remainingString.toString()));
					//Just write movieID::rating
					context.write(new Text(remainingString.toString()), new Text());				
				}
			}
		}
	}
	
	//Map class for movies file
	public static class Mapper_3 extends Mapper<LongWritable, Text, TextPair, Text>{
		private TextPair movieID = new TextPair();
	    private Text movieInfo = new Text();
	    
	    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
	    	//Use try-catch to avoid any faulty inputs
	    	try{
	    		String line = values.toString();
				line.trim();
				String[] token = line.split("::");
				movieID.set(new Text(token[0]), new Text("0"));
				movieInfo.set(token[1]+"::"+token[2]); //Append movieName and its Genre
				//Write <(movieID,0),(movieName::movieGenre)>
				context.write(movieID,movieInfo); //movieInfo = movieName::movieGenre
	    	}
	    	catch(Exception e){
	    		System.out.print("Found an incomplete entry in movies file...");
	    	}
		}
	}
	
	//Map class for users-ratings join file
	public static class Mapper_4 extends Mapper<LongWritable, Text, TextPair, Text>{
		private TextPair movieID = new TextPair();
	    private Text movieRating = new Text();
	    
	    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
	    	//Use try-catch to avoid any faulty inputs
	    	try{
	    		String line = values.toString();
				line.trim();
				String[] token = line.split("::");
				movieID.set(new Text(token[0]), new Text("1"));
				movieRating.set(token[1]);// Second field is movieRating.
				//Write <(movieID,0),(rating)>
				context.write(movieID,movieRating); //movieInfo = rating
	    	}
	    	catch(Exception e){
	    		System.out.print("Found an incomplete entry in movies file...");
	    	}
	    }
	}
	
	public static class Reducer_2 extends Reducer <TextPair, Text, Text, Text> {
		double cumulativeMovieRating=0.0;
		int loopVariable;
		String userGenre="" ;
		
		public boolean checkGenre(String inputGenre){
			boolean answer = false;
			String[] userGenreList = userGenre.split(":");//Genre List given by user
			String[] inputGenreList = inputGenre.split("\\|");//Genre List present in the input file
			
			int outerLoopVariable = 0;
			
			while(userGenreList.length > outerLoopVariable){
				int innerLoopVariable=0;
				while(inputGenreList.length > innerLoopVariable){
					if(inputGenreList[innerLoopVariable].equals(userGenreList[outerLoopVariable])){
						answer = true;
						return answer;
					}
					innerLoopVariable++;
				}
				outerLoopVariable++;
			}
			return answer;
		}
		
	    @Override   
	    //Method to get the custom movie genre given by the end user at run time
	    protected  void setup(Context context) throws IOException, InterruptedException
	    { 
	        Configuration conf = context.getConfiguration();   
	        userGenre = conf.get("userInput");   
	    }
		
		public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text movieID=new Text(key.getFirst());
			Iterator<Text> textIterator= values.iterator();
			Text movieInfo=new Text(textIterator.next());

			String[] movieDetails = (movieInfo.toString()).split("::"); //movieName::GenreList
			
			cumulativeMovieRating=0.0;
			loopVariable=0;
			while(textIterator.hasNext()) {
				cumulativeMovieRating += Double.parseDouble(textIterator.next().toString());
				loopVariable++;
			}
			if(loopVariable>0){
				cumulativeMovieRating = (cumulativeMovieRating/(double)loopVariable);
			}
			
			if( checkGenre(movieDetails[1]) && (cumulativeMovieRating<=4.7 && cumulativeMovieRating>=4.4)){
				context.write(new Text(movieID.toString()+"::"+movieInfo.toString()+"::"+String.valueOf(cumulativeMovieRating)), new Text());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Path usersInput = new Path(args[0]+"/users.dat");
		Path ratingsInput = new Path(args[0]+"/ratings.dat");
		Path moviesInput = new Path(args[0]+"/movies.dat");

		Path intermediateOutput = new Path(args[1]+"_intermediate");

		Path OutputFile = new Path(args[1]);
		
		Configuration conf_1 = new Configuration();        
		Job job_1 = new Job(conf_1, "homework_2_part_2_1");
		
		job_1.setJarByClass(homework_2_part_2.class);
		
		MultipleInputs.addInputPath(job_1, usersInput, TextInputFormat.class,Mapper_1.class);
		MultipleInputs.addInputPath(job_1, ratingsInput, TextInputFormat.class,Mapper_2.class);
		job_1.setMapOutputKeyClass(TextPair.class);
		job_1.setMapOutputValueClass(Text.class);
		job_1.setReducerClass(Reducer_1.class);
		job_1.setOutputFormatClass(TextOutputFormat.class);
		job_1.setOutputKeyClass(Text.class);
		job_1.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job_1, intermediateOutput);
		job_1.setGroupingComparatorClass(TextPair.FirstComparator.class);

		if(job_1.waitForCompletion(true)){
			Configuration conf_2 = new Configuration();
			conf_2.set("userInput",args[2]);
			Job job_2 = new Job(conf_2, "homework_2_part_2_2");
			job_2.setJarByClass(homework_2_part_2.class);
			   
			MultipleInputs.addInputPath(job_2, moviesInput, TextInputFormat.class,Mapper_3.class);
			MultipleInputs.addInputPath(job_2, intermediateOutput, TextInputFormat.class,Mapper_4.class);
			job_2.setMapOutputKeyClass(TextPair.class);
			job_2.setMapOutputValueClass(Text.class);
			job_2.setReducerClass(Reducer_2.class);
			job_2.setOutputFormatClass(TextOutputFormat.class);
			job_2.setOutputKeyClass(Text.class);
			job_2.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job_2, OutputFile);
			job_2.setGroupingComparatorClass(TextPair.FirstComparator.class);
			job_2.waitForCompletion(true);
		}
	}
}