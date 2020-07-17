package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class FilenameSumCompositeKey implements WritableComparable<FilenameSumCompositeKey> 
{
	private String filename;
	private String sum;

	public FilenameSumCompositeKey(String filename, String sum) 
	{
		this.filename = filename;
		this.sum = sum;
	}

	public FilenameSumCompositeKey() {}

	public void set(String filename, String sum) 
	{
		this.filename = filename;
		this.sum = sum;
	}

	public String getFilename() {return filename;}

	public String getSum() {return sum;}

	@Override
	public String toString() {return filename + " " + sum;}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		filename = in.readUTF();
		sum = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(filename);
		out.writeUTF(sum);
	}

	@Override
	public int compareTo(FilenameSumCompositeKey t) 
	{
		int cmp = this.filename.compareTo(t.filename);
		if (cmp != 0) 
		{
		    return cmp;
		}

		return this.filename.compareTo(t.sum);
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (obj == null) 
		{
		    return false;
		}

		if (getClass() != obj.getClass()) 
		{
		    return false;
		}

		final FilenameSumCompositeKey other = (FilenameSumCompositeKey) obj;

		if (this.filename != other.filename && (this.filename == null || !this.filename.equals(other.filename)))  
		{
		    return false;
		}

		if (this.sum != other.sum && (this.sum == null || !this.sum.equals(other.sum))) 
		{
		    return false;
		}

		return true;
	}

	@Override
	public int hashCode() 
	{
		return this.filename.hashCode() * 163 + this.sum.hashCode();
	}
}
