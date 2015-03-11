package org.ffeng.hazelcast;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;

public class ILockExample {
	static final String LIST_NAME = "to be locked";
    static final String LOCK_NAME = "to lock with";
    static final String CONDITION_NAME = "to signal with";
    
    public static class ListConsumer implements Runnable, Serializable, HazelcastInstanceAware {

		private static final long serialVersionUID = 1L;
		private transient HazelcastInstance instance;
        
        public void run() {
            ILock lock = instance.getLock(LOCK_NAME);
            ICondition condition = lock.newCondition(CONDITION_NAME);
            IList<Integer> list = instance.getList(LIST_NAME);
            lock.lock();
            try {
                while(list.isEmpty()) {
                    condition.await(2, TimeUnit.SECONDS);
                }
                while(!list.isEmpty()) {
                    System.out.println("value is " + list.get(0));
                    list.remove(0);
                }
            } catch(InterruptedException ie) {
                System.out.println("Consumer got interrupted");
            } finally {
                lock.unlock();
            }
            System.out.println("Consumer leaving");
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        	this.instance = hazelcastInstance;
    	}
    }
    
    public static class ListProducer implements Runnable, Serializable, HazelcastInstanceAware {
		private static final long serialVersionUID = 1L;
		private transient HazelcastInstance instance;

        public void run() {
            ILock lock = instance.getLock(LOCK_NAME);
            ICondition condition = lock.newCondition(CONDITION_NAME);
            IList<Integer> list = instance.getList(LIST_NAME);
            lock.lock();
            try {
                for(int i = 1; i <= 10; i++){
                    list.add(i);
                }
                condition.signalAll();
            } finally {
                lock.unlock();
            }
            System.out.println("Producer leaving");
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
        }
        
    }
    
	public static void main(String[] args) {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IExecutorService service = instance.getExecutorService("service");
        ListConsumer consumer = new ListConsumer();
        ListProducer producer = new ListProducer();
        
        try {
            service.submit(producer);
            service.submit(consumer);
            Thread.sleep(10000);
        } catch(InterruptedException ie){
            System.out.println("Got interrupted");
        } finally {
            instance.shutdown();
        }
	}

}
