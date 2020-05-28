package mapreduce.wcv3;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.xml.internal.bind.v2.schemagen.xmlschema.List;

import utils.PorterStemmer;

public class WordCount extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration myconf = getConf();
		myconf.set("textinputformat.record.delimiter", "\n[[");

		// The following two lines instruct Hadoop/MapReduce to run in local
		// mode. In this mode, mappers/reducers are spawned as thread on the
		// local machine, and all URLs are mapped to files in the local disk.
		// Remove these lines when executing your code against the cluster.
		myconf.set("mapreduce.framework.name", "local");
        myconf.set("fs.defaultFS", "file:///");

		Job job = Job.getInstance(myconf);
//		job.setJobName("MyWordCount(" + args[0] + ")");
		job.setJarByClass(WordCount.class);
		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyReducer.class);
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(job.getJobName() +
//				"_output"));

		ArrayList<String> stopwords = new ArrayList <String>();
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
		MyInputFormat f = new MyInputFormat();
		ArrayList<InputSplit> res = new ArrayList<InputSplit>();
		res.add((InputSplit) f.getSplits(job));
		
	//	for (int i = 0; i<res.size(); i++) {
	//		MyMapper m = new MyMapper();
		//	m.map(res.get(i), res.get(i)); //context?? how do we separate the key from the the value in res.get(i)?
	//	}
		
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

