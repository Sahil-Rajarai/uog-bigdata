package mapreduce.wcv2;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.PorterStemmer;

public class WordCount extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration myconf = getConf();

		// The following two lines instruct Hadoop/MapReduce to run in local
		// mode. In this mode, mappers/reducers are spawned as thread on the
		// local machine, and all URLs are mapped to files in the local disk.
		// Remove these lines when executing your code against the cluster.
		myconf.set("mapreduce.framework.name", "local");
        myconf.set("fs.defaultFS", "file:///");

		Job job = Job.getInstance(myconf, "WordCount-v3");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		List<String> stopwords = new ArrayList <String>();
		Scanner in = new Scanner(new File("src/main/resources/stopword-list.txt"));
		while(in.hasNextLine()) {
			String w = in.nextLine().trim();
			stopwords.add(w);
			
		}

		Scanner read_Sample = new Scanner(new File("src/main/resources/sample.txt"));
		File newInput = new File("newInput1.txt");
		PrintWriter out = new PrintWriter(newInput);
		PorterStemmer stemmer = new PorterStemmer();
		int title=0;
		System.out.println(stopwords.contains("a") + " " + "a");
		while(read_Sample.hasNext()) {
			String word = read_Sample.next();
			if(word.startsWith("[[")) {
				title = 1;
				out.write("\n");
			}
			
			if(title==1) {
				out.write(word);
				out.flush();
			} else if (stopwords.contains(word)== false){
				
					out.write(stemmer.stem(word));
					out.write(" ");
					out.flush();
				
				}
			if(word.endsWith("]]")) {
				title = 0;
				out.write("\n");
			}
			
			
		}
		
	/*	for (int i = 0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				job.addCacheFile(new Path(args[++i]).toUri());
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			} else
				other_args.add(args[i]);
		} */
		FileInputFormat.setInputPaths(job, new Path("src/main/resources/sample.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output"));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount(), args));
	}
}
