package com.java.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer<(입력키<Map출력키>: 입력값<Map출력값>) , (출력키<파일저장될 키워드> : 출력값<파일저장될 키워드의 수>)>
public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	// 출력값 변수
	IntWritable result = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// 결합할 값 변수
		int sum = 0;
		// 입력 받은 값을 반복문으로 합하기
		for(IntWritable v : value) {
			sum += v.get();
		}
		result.set(sum);
		// 전체 결과값을 출력하기
		context.write(key, result);
		
	}

}
