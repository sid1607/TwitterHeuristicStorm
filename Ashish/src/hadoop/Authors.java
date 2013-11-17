package hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Authors {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String a[] = line.split(":::");
			a[2] = a[2].substring(0, a[2].length()-2);
			String Aut[]= a[1].split("::");
			Text name = new Text(a[2]);
			for (int i=0;i<Aut.length;i++)
			{
				Text te = new Text(Aut[i]);
				context.write(te, name);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String temp = new String("");
			for (Text val : values)
			{
				
				temp = temp.concat(val.toString() + " ");
			}
			Text t = new Text(temp);
			context.write(key,t);
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		  Job job = new Job(conf, "wordcount");
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  job.setMapperClass(Map.class);
		  job.setReducerClass(Reduce.class);
		  job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  FileInputFormat.addInputPath(job, new Path("C:\\Users\\user\\Desktop\\hw3data"));
		  FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\user\\Desktop\\new"));
		  job.waitForCompletion(true);
	
	}

}