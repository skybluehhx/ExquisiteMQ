package com.lin.commons.cluster;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker集群
 * @author jianglinzou
 * @date 2019/3/11 下午1:01
 */
public class Cluster {
        private final ConcurrentHashMap<Integer/* broker id */, Set<Broker>> brokers =
                new ConcurrentHashMap<Integer, Set<Broker>>();

        transient private final static Random random = new Random();


        public ConcurrentHashMap<Integer, Set<Broker>> getBrokers() {
            return this.brokers;
        }


        /** 返回broker总数,包括master和slave */
        public int size() {
            int size = 0;
            for (Map.Entry<Integer/* broker id */, Set<Broker>> entry : this.brokers.entrySet()) {
                Set<Broker> brokers = entry.getValue();
                if (brokers != null) {
                    size = size + brokers.size();
                }
            }
            return size;
        }


        public Broker getBrokerRandom(int id) {
            Set<Broker> set = this.brokers.get(id);
            if (set == null || set.size() <= 0) {
                return null;
            }
            if (set.size() == 1) {
                return (Broker) set.toArray()[0];
            }
            // prefer master.
            for (Broker broker : set) {
                if (!broker.isSlave()) {
                    return broker;
                }
            }
            return (Broker) set.toArray()[random.nextInt(set.size())];
        }


        public Broker getMasterBroker(int id) {
            Set<Broker> set = this.brokers.get(id);
            if (set == null || set.size() <= 0) {
                return null;
            }
            for (Broker broker : set) {
                if (!broker.isSlave()) {
                    return broker;
                }
            }
            return null;
        }


        public void addBroker(int id, Broker broker) {
            Set<Broker> set = this.brokers.get(id);
            if (set == null) {
                set = new HashSet<Broker>();
                this.brokers.put(id, set);
            }
            set.add(broker);
        }


        public void addBroker(int id, Set<Broker> brokers) {
            Set<Broker> set = this.brokers.get(id);
            if (set == null) {
                set = new HashSet<Broker>();
                this.brokers.put(id, set);
            }
            set.addAll(brokers);
        }


        public Set<Broker> remove(int id) {
            return this.brokers.remove(id);
        }


        public Cluster masterCluster() {
            Cluster cluster = new Cluster();
            for (Map.Entry<Integer, Set<Broker>> entry : this.brokers.entrySet()) {
                Set<Broker> set = entry.getValue();
                if (set == null || set.isEmpty()) {
                    continue;
                }

                for (Broker broker : set) {
                    if (broker != null && !broker.isSlave()) {
                        cluster.addBroker(entry.getKey(), broker);
                    }
                }
            }
            return cluster;
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Cluster) {
                Cluster other = (Cluster) obj;
                return this.brokers.equals(other.brokers);
            }
            return false;
        }


        @Override
        public int hashCode() {
            return this.brokers.hashCode();
        }

}
