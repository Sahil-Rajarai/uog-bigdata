import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CompositeValue implements Writable {
	private String docID;
	private int freq;
	
	public CompositeValue() {
	
	}
	public CompositeValue(String docID, int freq) {
		this.docID = docID;
		this.freq = freq;
	}
	
	public String getDocID() {
		return this.docID;
	}
	public void setDocID(String docID) {
		this.docID = docID;
	}
	public int getFreq() {
		return this.freq;
	}
	public void setFreq(int freq) {
		this.freq = freq;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toString() {
		return "CompositeValue [docID=" + this.docID + ", freq=" + this.freq + "]";
	}
	
	
}
