package twittertest;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import twitter4j.internal.org.json.JSONException;
import twitter4j.internal.org.json.JSONObject;


public class WC_HashTagsJSON {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		//private Text word = new Text();
		
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			System.out.println(value.toString());
			JSONObject jsonob = new JSONObject(value);
			Iterator it = jsonob.keys();
			while(it.hasNext())
				System.out.println(it.next().toString());
			//jsonob.
			try {
				String tweet = jsonob.getString("filter_level");
				System.out.println(tweet);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				System.out.println("Text Not Found");
				e.printStackTrace();
			}
		}
		
	}
	
	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			Integer i=new Integer(0);
			for (IntWritable val : values)
			{
				i += val.get();
			}
			context.write(key,new Text(i.toString()));
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

		org.apache.hadoop.conf.Configuration conf = new Configuration();
		  Job job = new Job(conf, "wordcount");
		  job.setOutputKeyClass(IntWritable.class);
		  job.setOutputValueClass(Text.class);
		  job.setMapperClass(Map.class);
		  job.setReducerClass(Reduce.class);
		  job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  FileInputFormat.addInputPath(job, new Path("/media/ashish/OS/EclipseWorkspace/statusfilejson.json"));
		  FileOutputFormat.setOutputPath(job, new Path("/media/ashish/OS/EclipseWorkspace/wordcountoutput"));
		  job.waitForCompletion(true);
	
	}
}
