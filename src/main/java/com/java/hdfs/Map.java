package com.java.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper<(입력키<행번호> : 입력값<행의글자>) , (출력키<글자> : 출력값<1>)>
public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	// 출력 키 변수
	protected Text textKey = new Text();
	// 출력 값 변수
	protected IntWritable intValue = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		// 출력 키에 넣을 문자열 변수
		String strKey = values[0] + "year " + values[1] + "month";
		int intValues = Integer.parseInt(values[0]);
		
		if("NA".equals(values[15])) {
			values[15] = "0";
		}

		int a = Integer.parseInt(values[15]);
		
		if(intValues <= 1999 ) {
			intValue = new IntWritable(a);
		} else {
			intValue = new IntWritable(0);
		}
		
		// 출력 키에 문자열 변수 적용
		textKey.set(strKey);
		// 전체 결과 출력하기
		context.write(textKey, intValue);
	}
	
}
