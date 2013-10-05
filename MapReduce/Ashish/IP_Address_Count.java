package pack1;

import java.io.IOException;

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

public class IP_Address_Count {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		//private Text word = new Text();
		
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String ip_string="([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
			String a[] = line.split("\\[");
			String b;
			for (int i=0;i<a.length;i++)
			{
				if (a[i].indexOf(']')>-1)
				{
					b=a[i].substring(0, a[i].indexOf(']')-1);
					if (b.matches(ip_string))
					{
						Text te = new Text(b);
						context.write(te, new IntWritable(1));
					}
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
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
		  job.setOutputValueClass(IntWritable.class);
		  job.setMapperClass(Map.class);
		  job.setReducerClass(Reduce.class);
		  job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  FileInputFormat.addInputPath(job, new Path("C:\\Users\\user\\Desktop\\c.dns"));
		  FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\user\\Desktop\\new"));
		  job.waitForCompletion(true);
	
	}

}
