package com.elasticsearchDemo.elasticDemo.sourse;

import java.io.File;
import java.io.IOException;

public class FileSourse {
	
public String filepath() throws IOException {
		
	File file = new File("../resources/");
	String path = file.getCanonicalPath(); 
	return path;   

	}

}
