package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Hello extends UDF{
	
	public String evaluate(String str){
		return "hello world" + str; 
	}
}

