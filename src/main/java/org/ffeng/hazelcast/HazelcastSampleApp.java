package org.ffeng.hazelcast;

import java.util.Map;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;

public class HazelcastSampleApp {

	public static void main(String[] args) {
		HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        
        Map<Long, String> map = instance.getMap("a");
        IdGenerator gen = instance.getIdGenerator("gen");
        for(int i = 0; i < 10; i++) {
            map.put(gen.newId(), "stuff " + i);
        }
        
        Map<Long, String> map2 = instance2.getMap("a");
        for(Map.Entry<Long, String> entry: map2.entrySet()) {
            System.out.printf("entry: %d; %s\n", entry.getKey(), entry.getValue());
        }
        
        System.exit(0);
	}

}
