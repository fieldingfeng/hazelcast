package org.ffeng.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.IdGenerator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ISemaphoreExample {
	static final String SEMAPHORE_NAME = "to reduce access";
    static final String GENERATOR_NAME = "to use";
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IExecutorService service = instance.getExecutorService("service");
        List<Future> futures = new ArrayList(10);
        try {
            for(int i = 0; i < 10; i++) {
                futures.add(service.submit(new GeneratorUser(i)));
            }
            // so I wait til the last man.  No this may not be scalable.
            for(Future future: futures) {
                future.get();
            }
        } catch(InterruptedException ie){
            System.out.printf("Got interrupted.");
        } catch(ExecutionException ee) {
            System.out.printf("Cannot execute on Future. reason: %s\n", ee.toString());
        } finally {
            service.shutdown();
            instance.shutdown();
        }

    }
    
    static class GeneratorUser implements Callable, Serializable, HazelcastInstanceAware {
        private transient HazelcastInstance instance;
        private final int number;
        
        public GeneratorUser(int number) {
            this.number = number;
        }
        
        public Long call() {
            ISemaphore semaphore = instance.getSemaphore(SEMAPHORE_NAME);
            IdGenerator gen = instance.getIdGenerator(GENERATOR_NAME);
            long lastId = -1;
            try {
                semaphore.acquire();
                try {
                    for(int i = 0; i < 10; i++){
                        lastId = gen.newId();
                        System.out.printf("current value of generator on %d is %d\n", number, lastId);
                        Thread.sleep(1000);
                    }
                } catch(InterruptedException ie) {
                    System.out.printf("User %d was Interrupted\n", number);
                } finally {
                    semaphore.release();
                }
            } catch(InterruptedException ie) {
                System.out.printf("User %d Got interrupted\n", number);
            }
            System.out.printf("User %d is leaving\n", number);
            return lastId;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
        }
        
    }

}
