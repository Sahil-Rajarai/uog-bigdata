	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.conf.Configured;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.util.Tool;
	import org.apache.hadoop.util.ToolRunner;

 
public class MyIndexer extends Configured implements Tool{
		@Override
		public int run(String[] args) throws Exception {
			
			Configuration myconf = getConf();
			myconf.set("TextInputFormat.record.delimiter", "\n[["); //setting the delimiter of each record of the input passed.
			myconf.set("docID", "");
			// The following two lines instruct Hadoop/MapReduce to run in local
			// mode. In this mode, mappers/reducers are spawned as thread on the
			// local machine, and all URLs are mapped to files in the local disk.
			// Remove these lines when executing your code against the cluster.
			myconf.set("mapreduce.framework.name", "local");
	        myconf.set("fs.defaultFS", "file:///");

			Job job = Job.getInstance(myconf);
			job.setJobName("Indexer");
			job.setJarByClass(MyIndexer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapperClass(MyMapper.class);
			job.setPartitionerClass(MyPartitioner.class);
			job.setMapOutputKeyClass(Text.class); //
			job.setMapOutputValueClass(IntWritable.class); //
			job.setReducerClass(MyReducer.class);
			job.setCombinerClass(MyReducer.class);
			
			MultipleOutputs.addNamedOutput(job, "wordFreq", TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "docLength", TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "nt", TextOutputFormat.class, Text.class, Text.class);
//			FileInputFormat.setInputPaths(job, new Path(args[0])); 			//on cluster 
			FileInputFormat.setInputPaths(job, new Path("src/main/resources/sample.txt")); //passing the new input file to the job.
			FileOutputFormat.setOutputPath(job, new Path(job.getJobName() + "_out"));
	
			int task = job.waitForCompletion(true) ? 0 : 1;
			
			Counters c = job.getCounters();
			Long num_docs = c.findCounter(MyMapper.Counters.NUM_RECORDS).getValue();
			System.out.println("numDocs : " + num_docs);
			Counters c1 = job.getCounters();
			Long sum_len = c.findCounter(MyMapper.Counters.NUM_BYTES).getValue();
			Long avg_d = sum_len/num_docs;
			System.out.println("The average document length is: " + avg_d);
			
			return task;
		}

		public static void main(String[] args) throws Exception {
			System.exit(ToolRunner.run(new Configuration(), new MyIndexer(), args));
		}
	}



