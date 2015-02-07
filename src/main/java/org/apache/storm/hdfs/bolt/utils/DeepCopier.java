package org.apache.storm.hdfs.bolt.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;

public class DeepCopier {
	/**
	 * Returns a copy of the object, or null if the object cannot
	 * be serialized.
	 */
	public static Object copy(Object orig) {
	    Object obj = null;
	    try {
	        // Write the object out to a byte array
	        ByteArrayOutputStream bos = new ByteArrayOutputStream();
	        ObjectOutputStream out = new ObjectOutputStream(bos);
	        out.writeObject(orig);
	        out.flush();
	        out.close();
	
	        // Make an input stream from the byte array and read
	        // a copy of the object back in.
	        ObjectInputStream in = new ObjectInputStream(
	            new ByteArrayInputStream(bos.toByteArray()));
	        obj = in.readObject();
	        in.close();
	    }
	    catch(IOException e) {
	        e.printStackTrace();
	    }
	    catch(ClassNotFoundException cnfe) {
	        cnfe.printStackTrace();
	    }
	    return obj;
	}
}

