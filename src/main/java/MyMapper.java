
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.StringUtils;
import java.util.StringTokenizer;
import utils.PorterStemmer;

/*The Mapper is modified such that one of its outputs is a CompositeValue representing the two different key-value pairs of the mapper.
 One output would be the (term, <docID,1>) representing the ft,d (term frequency),
 while the other would be <docID,1> representing |d| (document length).
 */
public class MyMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
	//defining the counters needed. NUM_RECORDS represents n, NUM_BYTES represents sum of all lengths of the documents.
	public static enum Counters { NUM_RECORDS, NUM_LINES, NUM_BYTES, INPUT_WORDS } 
	private Text _key = new Text();
	MultipleOutputs multipleOutputs; //to write to the two separate output files as required in the specification.
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private boolean caseSensitive = true;
	private Set<String> patternsToSkip = new HashSet<String>();
	private long numRecords = 0;
	private String inputFile;
	private String document_id;

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		FileSystem in = FileSystem.get(conf);
		Path p = new Path("src/main/resources/stopword-list.txt");
		document_id = conf.get("docID"); //keep the document_id as a global variable
		BufferedReader fis = new BufferedReader(new	InputStreamReader(in.open(p)));
		String pattern = null;
		while ((pattern = fis.readLine()) != null)
			this.patternsToSkip.add(pattern);
			fis.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
//		String document_id ="";
		HashMap<Text, IntWritable> mapValues = new HashMap<Text, IntWritable>();
		PorterStemmer stemmer = new PorterStemmer();
		String doc = "d_"; //prefixing the key to help the reducer distinguish between the two outputs.
		String term = "t_"; //prefixing the key to help the reducer distinguish between the two outputs.
		
		String line = value.toString();
		if(!(line.isEmpty()) && line.startsWith("[[") && line.endsWith("]]")) {
			document_id = line.toString();
			conf.set("docID", document_id); //set the document_id
		}
		else {
//			document_id = conf.get("docID");
//			System.out.print("documentId else" + document_id + "\n");
			StringTokenizer tokenizer = new StringTokenizer(line," ");
			while (tokenizer.hasMoreTokens()) {
				this.word.set(tokenizer.nextToken());
				if(this.patternsToSkip.contains(this.word)) {
					continue;
				}
				this.word = new Text(stemmer.stem(this.word.toString()));
				this._key = new Text(term+this.word); //prefixing the token based on its type.
				//generating the ft,d output as (term,{document_id,1})
//				System.out.print("documentId else" + document_id + "\n");
				context.write(new Text(doc+document_id), new IntWritable(1));
				context.getCounter(Counters.INPUT_WORDS).increment(1);
				mapValues.put(new Text(term+this.word+"d_"+document_id), new IntWritable(1));
				
			}
		}
		
		for(Text t : mapValues.keySet()) {
//			System.out.println(t + " " + mapValues.get(t));
			context.write(t, mapValues.get(t));
		}
		
		context.getCounter(Counters.NUM_LINES).increment(1);
		context.getCounter(Counters.NUM_BYTES).increment(value.getLength()); //Sum of all lengths of all documents
		context.getCounter(Counters.NUM_RECORDS).increment(1); //incrementing the map records
		if ((++this.numRecords % 100) == 0)
			context.setStatus("Finished processing " + this.numRecords + " records " +
					"from the input file: " + this.inputFile);
			
	}
}
