package com.liveperson.dataaccess;

import java.io.*;
import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;


public class Generate {
	public static void Deserialize() throws IOException {      
				
		File opfile = new File("output.csv");
		if (opfile.exists()) {
			opfile.delete();
		}
		
		PrintWriter printwrite = new PrintWriter(new FileWriter("output.csv", true), true);
		
		DatumReader<MetaData> userDatumReader = new SpecificDatumReader<MetaData>(MetaData.class);
		DataFileReader<MetaData> dataFileReader = null;		
		String fileName = "livePerson.avro";
		ClassLoader classLoader = ClassLoader.getSystemClassLoader();
		File file = new File(classLoader.getResource(fileName).getFile());
		MetaData metadata = null;
		try {
			dataFileReader = new DataFileReader<MetaData>(file, userDatumReader);        
			while (dataFileReader.hasNext()) {
			metadata = dataFileReader.next(metadata);
			System.out.println(metadata);
			
			printwrite.write(metadata.toString());
			printwrite.write("\n");
			
			}
			printwrite.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
	Deserialize();
	}
}
