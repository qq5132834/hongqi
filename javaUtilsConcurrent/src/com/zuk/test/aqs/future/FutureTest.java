package com.zuk.test.aqs.future;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FutureTest {

	static class MyCallable implements Callable<String>{

		@Override
		public String call() throws Exception {
			System.out.println("do something.");
			Thread.sleep(3000);
			return "helloWorld.";
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		ExecutorService exec = Executors.newCachedThreadPool();
		Future<String> future = exec.submit(new MyCallable());
		System.out.println("do something in main.");
		Thread.sleep(1000);
		String res = future.get();
		System.out.println("res:" + res);
	}
	
}
