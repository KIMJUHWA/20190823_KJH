package com.java.hdfs;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hadoop {
	
	// 결과 내역 담을 객체
	protected HashMap<String, Object> resultMap;
	// local 및 hadoop 설정 객체 
	protected Configuration hadoopConf = null;
	protected Configuration localConf = null;
	// hadoop 접속 주소 (hadoop server ip 수정 할것) <<<<<<<<<<<<<<<<<<
	protected final String URL = "hdfs://192.168.3.79:9000";
	protected final String LOCAL = "C:\\Users\\GD7\\Desktop\\data/";
	// hadoop 정제 대상 경로 / 처리 결과 저장 경로 및 파일
	protected final String INPUT = "/input/";
	protected final String OUTPUT = "/output";
	protected final String TARGET = "/part-r-00000";
	// hadoop 정제 대상 경로 및 처리 경로 객체
	protected Path inputPath = null;
	protected Path outputPath = null;
	// local 저장소 & hadoop 저장소 객체
	protected FileSystem hadoopSystem = null;
	protected FileSystem localSystem = null;
	
	// Hadoop 외부 요청 메소드
	public HashMap<String, Object> run(String fileName){
		System.out.println("Hadoop.run() >> Start");
		resultMap = new HashMap<String, Object>();
		int status = 0;
		// Hadoop 시스템 접속 하기 위하여 확인 요청
		if(init(fileName)) {
			/**************************************************
			 * >> 상태값 설정 << 
			 * 0 : 접속 오류 (Hadoop 연결 문제 발생)
			 * 1 : 정제 오류 (MapReduce 처리 문제 발생)
			 * 2 : 처리 완료 (전체 정상 처리)
			 * 
			 * >> 진행 순서 <<
			 * 1) 파일 복사 : fileCopy()
			 * 2) 정제 요청 : mapReduser()
			 * 3) 성공 시 결과 받기 : resultData()
			 **************************************************/
			if(fileCopy(fileName)) {
				try {	// mapReduser의 책임전가 예외처리
					if(mapReduser()) {
						try {	// resultData의 책임전가 예외처리
							resultMap.put("result", resultData());
							status = 2;
						} catch (IOException e) {
							e.printStackTrace();
							status = 1;
							System.out.println(e + " : 파일 읽기 입출력오류");
						}
					}
				} catch (ClassNotFoundException e) {
					status = 1;
					System.out.println(e + " : 클래스 오류");
				} catch (IOException e) {
					status = 1;
					System.out.println(e + " : 입출력 오류");
				} catch (InterruptedException e) {
					status = 1;
					System.out.println(e + " : 인터럽트 오류");
				} catch (Exception e) {
					status = 1;
					System.out.println(e + " : 알수없는 오류");
				}
			}
		}
		resultMap.put("status", status);
		System.out.println("Hadoop.run() >> End");
		return resultMap;
	}
	
	// Hadoop 시스템 접속 기본 정의 요청 메소드
	protected boolean init(String fileName) {
		System.out.println("Hadoop.init() >> Start");
		boolean status = true;
		try {
			// 접속 설정 객체 변수
			localConf = new Configuration();
			hadoopConf = new Configuration();
			hadoopConf.set("fs.defaultFS", URL);
			
			// Hadoop 정제 시 사용 할 경로 객체 정의
			inputPath = new Path(INPUT + fileName);
			outputPath = new Path(OUTPUT);
			
			// 파일시스템 정보 정의
			localSystem = FileSystem.getLocal(localConf);
			hadoopSystem = FileSystem.get(hadoopConf);
		} catch (Exception e) {
			e.printStackTrace();
			status = false;
		}
		System.out.println("Hadoop.init() >> End");
		return status;
	}
	
	// 분석 대상 파일 Hadoop에 저장
	protected boolean fileCopy(String fileName) {
		System.out.println("Hadoop.fileCopy() >> Start");
		boolean status = true;
		try {
			// 로컬 저장소에 있는 경로 파일 객체
			Path filePath = new Path(LOCAL + fileName);
			// 원본 데이터 열기
			FSDataInputStream fsis = localSystem.open(filePath);
			// 대상 복사 만들기 
			FSDataOutputStream fsos = hadoopSystem.create(inputPath);
			int byteRead = 0;
			// 원본 데이터 한줄씩 읽어 오기
			while ((byteRead = fsis.read()) > 0) {
				// 대상 복사 파일에 넣기
				fsos.write(byteRead);					
			}
			fsis.close();
			fsos.close();
		} catch (Exception e) {
			e.printStackTrace();
			status = false;
		}
		System.out.println("Hadoop.fileCopy() >> End");
		return status;
	}
	
	// Hadoop 정제 요청 메소드
	// 하둡을 실행시키기 위해서 씀 맵과 리듀스 씀
	protected boolean mapReduser() throws ClassNotFoundException, IOException, InterruptedException {
		System.out.println("Hadoop.mapReduser() >> Start");
		// 정제 작업 객체 변수
		Job job = Job.getInstance(hadoopConf, "test");
		// 실행 대상 클래스 지정
		job.setJarByClass(Hadoop.class);
		// Mapper 객체 지정
		job.setMapperClass(Map.class);
		// Resucer 객체 지정
		job.setReducerClass(Reduce.class);
		// Mapper 객체 출력 (키, 값) 정의
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 정제 결과 출력 (키, 값) 정의
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 작업 시 생성 될 테스크 정의
		job.setNumReduceTasks(1);
		// 원본 및 대상 경로 정의
		FileInputFormat.addInputPath(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
		System.out.println("Hadoop.mapReduser() >> End");
		// 처리 결과 보내기
		return job.waitForCompletion(true);
	}
	
	// 정제 결과 데이터 가져오기 메소드
	protected String resultData() throws IOException {
		System.out.println("Hadoop.resultData() >> Start");
		// 정제 결과 데이터 경로 객체 생성
		Path targetPath = new Path(OUTPUT + TARGET);
		// 결과 문자열에 담기 위한 변수
		StringBuilder sb = new StringBuilder();
		// 정제 결과 경로에 존재 여부 확인
		if(hadoopSystem.exists(targetPath)){
			// 정제 결과 대상 파일 읽어 오기
			FSDataInputStream fsis = hadoopSystem.open(targetPath);
			int byteRead = 0;
			while((byteRead = fsis.read()) > 0) { 
				// 정제 결과를 문자열 변수에 담기
				// char로 아스키코드 형변환 가능, char의 특징
				sb.append((char)byteRead);
			}
			fsis.close();
		}
		System.out.println("Hadoop.resultData() >> End");
		return sb.toString();
	}

}
