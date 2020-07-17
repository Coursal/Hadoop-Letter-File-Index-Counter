package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class LetterFilenameCompositeKey implements WritableComparable<LetterFilenameCompositeKey> 
{
	private String letter;
	private String filename;

	public LetterFilenameCompositeKey(String letter, String filename) 
	{
		this.letter = letter;
		this.filename = filename;
	}

	public LetterFilenameCompositeKey() {}

	public void set(String letter, String filename) 
	{
		this.letter = letter;
		this.filename = filename;
	}

	public String getLetter() {return letter;}

	public String getFilename() {return filename;}

	@Override
	public String toString() {return filename + " " + letter;}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
		letter = in.readUTF();
		filename = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(letter);
		out.writeUTF(filename);
	}

	@Override
	public int compareTo(LetterFilenameCompositeKey t) 
	{
		int cmp = this.letter.compareTo(t.letter);
		if (cmp != 0) 
		{
		    return cmp;
		}

		return this.filename.compareTo(t.filename);
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

		final LetterFilenameCompositeKey other = (LetterFilenameCompositeKey) obj;

		if (this.letter != other.letter && (this.letter == null || !this.letter.equals(other.letter))) 
		{
		    return false;
		}

		if (this.filename != other.filename && (this.filename == null || !this.filename.equals(other.filename))) 
		{
		    return false;
		}

		return true;
	}

	@Override
	public int hashCode() 
	{
		return this.letter.hashCode() * 163 + this.filename.hashCode();
	}
}
