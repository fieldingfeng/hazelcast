package org.ffeng.hazelcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;

public class HazelcastIExecutorServiceExamples {
	private static final Logger logger = LoggerFactory.getLogger(HazelcastIExecutorServiceExamples.class);
	public static final String SERVICE_NAME = "spinnerella";
    public static final int NUM_INSTANCES = 5;
    
    enum HazelcastIExecutorServices {
    	TO_SOME_MEMBER () {
			@Override
			public void example(List<HazelcastInstance> instances,
					IExecutorService spinner) throws ExecutionException, InterruptedException {
				logger.info("Submit to some member.");
	            Future<Integer> howMany = spinner.submit(new MoveItMoveIt());
	            logger.info("It moved it {} times", howMany.get());
	            setDone(true);
				
			}
    	},
    	TO_PARTICULAR_MEMBER () {

			@Override
			public void example(List<HazelcastInstance> instances,
					IExecutorService spinner) throws ExecutionException, InterruptedException {
				logger.info("Submit to a particular member.");
	            Member member = getRandomMember(instances);
	            logger.debug("member is {}", member);
	            Future<Integer> howMany = spinner.submitToMember(new MoveItMoveIt(), member);
	            logger.info("It moved it {} times.", howMany.get());
	            setDone(true);
			}

			private Member getRandomMember(List<HazelcastInstance> instances) {
	            Set<Member> members = instances.get(0).getCluster().getMembers();
	            int i = 0;
	            int max = new Random().nextInt(instances.size());
	            Iterator<Member> iterator = members.iterator();
	            Member member = iterator.next();
	            while(iterator.hasNext() && (i < max)) {
	                member = iterator.next();
	                i++;
	            }
	            return member;
	        }
    	},
    	ON_A_SET_OF_MEMBERS() {

			@Override
			public void example(List<HazelcastInstance> instances,
					IExecutorService spinner) throws ExecutionException, InterruptedException {
				logger.info("Send to some of the members");
	            Set<Member> randomMembers = getRandomMembers(instances);
	            Map<Member, Future<Integer>> results = 
	                    spinner.submitToMembers(new MoveItMoveIt(), randomMembers);
	            for(Future<Integer> howMany: results.values()) {
	                logger.info("It moved {} times", howMany.get());
	            }
	            setDone(true);
			}
    		
			private Set<Member> getRandomMembers(List<HazelcastInstance> instances) {
	            int max = new Random().nextInt(instances.size());
	            Set<Member> newSet = new HashSet<>(instances.size());
	            int k = 0;
	            Iterator<Member> i = instances.get(0).getCluster().getMembers().iterator();
	            while(i.hasNext() && k < max) {
	                newSet.add(i.next());
	                k++;
	            }
	            return newSet;
	        }
    	}, 
    	ON_THE_KEY_OWNER() {

			@Override
			public void example(List<HazelcastInstance> instances,
					IExecutorService spinner) throws ExecutionException, InterruptedException {
				logger.info("Send to the one owning the key");
	            HazelcastInstance randomInstance = getRandomInstance(instances);
	            IMap<Long, Boolean> map = randomInstance.getMap("default");
	            Long one = 1L;
	            map.put(one, Boolean.TRUE);
	            
	            Future<Integer> howMany = spinner.submitToKeyOwner(new MoveItMoveIt(), one);
	            logger.info("It moved it {} times.", howMany.get());
	            setDone(true);
			}
    		
			private HazelcastInstance getRandomInstance(List<HazelcastInstance> instances) {
	            return instances.get(new Random().nextInt(instances.size()));
	        }
    	},
    	ON_ALL_MEMBERS() {

			@Override
			public void example(List<HazelcastInstance> instances,
					IExecutorService spinner) throws ExecutionException, InterruptedException {
				logger.info("Send to all members");
	            Map<Member, Future<Integer>> results = 
	                    spinner.submitToAllMembers(new MoveItMoveIt());
	            for(Future<Integer> howMany: results.values()) {
	                logger.info("It moved {} times", howMany.get());
	            }
	            setDone(true);
				
			}
    		
    	},
    	CALLBACK() {

			@Override
			public void example(List<HazelcastInstance> instances,
					IExecutorService spinner) throws ExecutionException, InterruptedException {
				logger.info("example with a callback");
	            spinner.submit(new MoveItMoveIt(), new ExecutionCallback<Integer>() {
	                @Override
	                public void onResponse(Integer response) {
	                    logger.info("It moved {} times", response);
	                    setDone(true);
	                }

	                @Override
	                public void onFailure(Throwable thrwbl) {
	                    logger.error("trouble in the callback", thrwbl);
	                    setDone(true);
	                }
	            });
			}
    		
    	},
    	MULTIPLE_MEMBERS_WITH_CALLBACK() {

			@Override
			public void example(List<HazelcastInstance> instances,
					IExecutorService spinner) throws ExecutionException, InterruptedException {
				logger.info("running on multiple members with callback");
	            spinner.submitToAllMembers(new MoveItMoveIt(), new MultiExecutionCallback() {

	                @Override
	                public void onResponse(Member member, Object o) {
	                    logger.info("member finished with {} moves", o);
	                }

	                @Override
	                public void onComplete(Map<Member, Object> map) {
	                    logger.info("All members completed");
	                    for(Object value: map.values()) {
	                        logger.info("It moved {} times", value);
	                    }
	                    setDone(true);
	                }
	            });				
			}
    		
    	};
    	
    	private boolean done;
    	public abstract void example(List<HazelcastInstance> instances, IExecutorService spinner) throws ExecutionException, InterruptedException;
    	
    	public boolean isDone() {
    		return done;
    	}
    	
    	public void setDone(boolean done) {
    		this.done = done;
    	}
    }
    
	public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "slf4j");
        List<HazelcastInstance> instances = new ArrayList<>(NUM_INSTANCES);
        for(int i = 0; i < NUM_INSTANCES; i++) {
            instances.add(Hazelcast.newHazelcastInstance());
            logger.info("instance {} up", i);
        }		

        IExecutorService spinner = instances.get(0).getExecutorService(SERVICE_NAME);
        try {
//            HazelcastIExecutorServices.TO_SOME_MEMBER.example(instances, spinner);
//            HazelcastIExecutorServices.TO_PARTICULAR_MEMBER.example(instances, spinner);
//            HazelcastIExecutorServices.ON_THE_KEY_OWNER.example(instances, spinner);
            HazelcastIExecutorServices.ON_A_SET_OF_MEMBERS.example(instances, spinner);
//            HazelcastIExecutorServices.ON_ALL_MEMBERS.example(instances, spinner);
//            HazelcastIExecutorServices.CALLBACK.example(instances, spinner);
//            HazelcastIExecutorServices.MULTIPLE_MEMBERS_WITH_CALLBACK.example(instances, spinner);
            
            //Lets setup a loop to make sure they are all done (Especially the callback ones)
//            for(HazelcastIExecutorServices example: HazelcastIExecutorServices.values()) {
//                while(!example.isDone()) {
//                    Thread.sleep(1000);
//                }
//            }
            while(!HazelcastIExecutorServices.ON_A_SET_OF_MEMBERS.isDone()) {
            	Thread.sleep(1000);
            }
            
        } catch(ExecutionException ee) {
            logger.warn("Can't finish the job", ee);
        } catch(InterruptedException ie) {
            logger.warn("Everybody out of the pool", ie);
        } finally {
            // time to clean up my toys
            boolean allClear = false;
            
            while(!allClear) {
                try {
                    Thread.sleep(1000);
                    Hazelcast.shutdownAll();
                    allClear = true;
                } catch(InterruptedException ie) {
                    //got interrupted. try again
                } catch(RejectedExecutionException ree) {
                    logger.debug("caught a RejectedExecutionException");
                    allClear = false;
                }
            }
            
            logger.info("All done");
        }
	}

	
	public static class MoveItMoveIt implements Callable<Integer>, Serializable {
	    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		private static final Logger logger = LoggerFactory.getLogger(MoveItMoveIt.class);
	    private static final int UPPER_BOUND = 15;
	        
	    @Override
	    public Integer call() throws Exception {
	        Random random = new Random();
	        int howMany = random.nextInt(UPPER_BOUND);
//	        int howMany = 2;
	        for(int i = 0; i < howMany; i++) {
	            logger.info("I like to Move it Move it!");
	        }
	        logger.info("Move it!");
	        return howMany;
	    }
	}
}
