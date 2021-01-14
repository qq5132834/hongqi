package com.zuk.test.aqs.lock;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConditionTest {
	
	static Lock reentrantLock = new ReentrantLock();
	static Condition condition = reentrantLock.newCondition();
	
	public static void main(String[] args) {
		
		new Thread(()->{
			reentrantLock.lock();
			try{
				System.out.println("wait signal.");
				condition.await();
			} catch( Exception e ){
				
			} finally{
				System.out.print("get signal.");
				reentrantLock.unlock();
			}
		}).start();
		
		new Thread(() -> {
			reentrantLock.lock();
			System.out.println("get lock");
			try{
				Thread.sleep(5000);
			}catch(Exception e){
				
			}finally{
				condition.signalAll();
				System.out.println(" send signal. ");
				reentrantLock.unlock();
			}
		}).start();
		
	}
}
