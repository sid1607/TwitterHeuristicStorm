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

public class WordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		//private Text word = new Text();
		
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			line = line.substring(1, line.length()-1);
			String a[] = new String[2];
			a = line.split(",",2);
			a[0] = a[0].substring(1,a[0].length()-1);
			Text word = new Text(a[0]);
			a[1] = a[1].substring(4,a[1].length()-3);
			String words[] = a[1].split(" ");
			for(int i=0;i<words.length;i++)
			{
				if(words[i].length()>1)
				{
					Text te = new Text(words[i]);
					context.write(te, word);
				}
			}
		}
		
	}
	
	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String temp = new String("");
			for (Text val : values)
			{
				temp = temp.concat(val.toString()+" ");
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
		  FileInputFormat.addInputPath(job, new Path("C:\\Users\\user\\Desktop\\book.txt"));
		  FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\user\\Desktop\\new.txt"));
		  job.waitForCompletion(true);
	
	}

}
