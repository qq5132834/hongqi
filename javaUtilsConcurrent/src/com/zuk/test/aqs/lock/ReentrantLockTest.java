package com.zuk.test.aqs.lock;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReentrantLockTest {
	
	private final Map<String, Data> map = new TreeMap<String, ReentrantLockTest.Data>();
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock readLock = lock.readLock();
	private final Lock writeLock = lock.writeLock();
	public Data get(String key){
		readLock.lock();
		try{
			return map.get(lock);
		} finally{
			readLock.unlock();
		}
	}
	public Set<String> getAllKeys(){
		readLock.lock();
		try {
			return map.keySet();
		} finally{
			readLock.unlock();
		}
		
	}
	public Data put(String key, Data data){
		writeLock.lock();
		try {
			return map.put(key, data);
		} finally{
			writeLock.unlock();
		}
	}
	
	public static void main(String[] args) {
		
	}
	
	class Data{
		
	}

}
