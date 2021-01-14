package com.zuk.test.aqs.future;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class FutrueTaskTest {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		FutureTask<String> futureTask 
			= new FutureTask<String>(new Callable<String>() {

				@Override
				public String call() throws Exception {
					System.out.println("do something.");
					Thread.sleep(3000);
					return "helloWorld.";
				}
		});
		
		new Thread(futureTask).start();
		System.out.println("do something in mian.");
		String res = futureTask.get();
		System.out.println("res:" + res);
		
	}
}
