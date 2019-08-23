package com.java;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.java.hdfs.Hadoop;

@WebServlet("/Home")
public class Home extends HttpServlet {
	
	// 시작 화면 출력
	protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		System.out.println("Home.doGet() >> Start");
		RequestDispatcher rd = req.getRequestDispatcher(viewPath("home"));
		rd.forward(req, res);
		
		System.out.println("Home.doGet() >> End");
	}

	// 분석 및 결과 출력
	protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		System.out.println("Home.doPost() >> Start");
		// 정제 요청 대상 파일명 변수
		String file_name = req.getParameter("file_name");		
		if(file_name == null || ("").equals(file_name)) {
			// 정제 요청 대상 파일명 값이 없으면 Home 화면 요청
			res.sendRedirect("/Home");
		} else {
			// 정제 요청 대상 파일명 값이 있으면 HDFS 실행 요청 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
			/***************/
			Hadoop hadoop = new Hadoop();
			HashMap<String, Object> resultMap = hadoop.run(file_name);
			
			int status = Integer.parseInt(resultMap.get("status").toString());
			
			String result = "";
			if(status == 0) {
				result = "접속 오류";
			} else if(status == 1) {
				result = "정제 오류";
			} else if(status == 2) {
				 result = resultMap.get("result").toString();
			} else {
				result = "예상치 못한 오류";
			}
			
			req.setAttribute("file_name", file_name);
			req.setAttribute("result", result);
			/***************/
			RequestDispatcher rd = req.getRequestDispatcher(viewPath("result"));
			rd.forward(req, res);
		}
		System.out.println("Home.doPost() >> End");
	}

	// 웹 화면 처리 메소드
	protected String viewPath(String view) {
		// 화면 파일 경로
		String prefix = "/WEB-INF/views/";
		// 화면 파일 확장자
		String suffix = ".jsp"; 
		return prefix + view + suffix;
	}
}
