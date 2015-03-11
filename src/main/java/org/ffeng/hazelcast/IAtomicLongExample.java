package org.ffeng.hazelcast;

import java.io.Serializable;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;

public class IAtomicLongExample {
	public static class MultiplyByTwoAndSubtractOne implements IFunction<Long, Long>, Serializable {

		private static final long serialVersionUID = 1L;

		public Long apply(Long input) {
			return 2*input - 1;
		}
		
	}
	
	public static void main(String[] args) {
		HazelcastInstance instance = Hazelcast.newHazelcastInstance();
		final String NAME = "atomic";
		IAtomicLong aLong = instance.getAtomicLong(NAME);
        IAtomicLong bLong = instance.getAtomicLong(NAME);
        aLong.getAndSet(1L);
        System.out.println("bLong is now: " + bLong.getAndAdd(2));
        System.out.println("aLong is now: " + aLong.getAndAdd(0L));

        MultiplyByTwoAndSubtractOne alter = new MultiplyByTwoAndSubtractOne();
        aLong.alter(alter);
        System.out.println("bLong is now: " + bLong.getAndAdd(0L));
        bLong.alter(alter);
        System.out.println("aLong is now: " + aLong.getAndAdd(0L));
        
        System.exit(0);
	}

}
