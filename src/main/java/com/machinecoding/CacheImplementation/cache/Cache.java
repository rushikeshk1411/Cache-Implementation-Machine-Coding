package com.machinecoding.CacheImplementation.cache;

import ch.qos.logback.core.boolex.EvaluationException;
import com.machinecoding.CacheImplementation.DataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Cache<KEY, VALUE> {

    private final Map<KEY, Record<VALUE>> map;
    private final DataSource<KEY, VALUE> dataSource;
    private final PersistanceAlgorithm persistanceAlgorithm;
    private final EvictionAlgorithm evictionAlgorithm;
    private final Integer expirationTimeInMilliSecond;
    private final Map<Long, List<Record<VALUE>>> expiryQueue;
    private final Map<AccessDetails, List<Record<VALUE>>> priorityQueue;
    private static final Integer THRESHHOLD_SIZE = 500;

    public Cache(DataSource<KEY, VALUE> dataSource, PersistanceAlgorithm persistanceAlgorithm, EvictionAlgorithm evictionAlgorithm, Integer expirationTimeInMilliSecond) {
        this.map = new ConcurrentHashMap<>();
        this.dataSource = dataSource;
        this.persistanceAlgorithm = persistanceAlgorithm;
        this.evictionAlgorithm = evictionAlgorithm;
        this.expirationTimeInMilliSecond = expirationTimeInMilliSecond;
        this.expiryQueue = new ConcurrentSkipListMap<>();
        this.priorityQueue = new ConcurrentSkipListMap<>((first, second) -> {
            var older = (int)(first.getAccesTimeStamps() - second.getAccesTimeStamps());
            if(evictionAlgorithm.equals(EvictionAlgorithm.LRU))
                return older;
            else
                return (int)(Objects.equals(first.getAccessCount(), second.getAccessCount()) ? older : first.getAccessCount() - second.getAccessCount());
        });
    }

    public CompletableFuture<VALUE> get(KEY key){
        final CompletableFuture<Record<VALUE>> result;
        if(map.containsKey(key) && map.get(key).getAccessDetails().getAccesTimeStamps() >= System.currentTimeMillis() - expirationTimeInMilliSecond){
            result = CompletableFuture.completedFuture(map.get(key));
        }else{
            result = dataSource.get(key).thenApply(value -> addToCache(key, value));
        }

         return result.thenApply((valueRecord) -> {
             final var accessDetails = valueRecord.getAccessDetails();
             accessDetails.setAccessCount(accessDetails.getAccessCount()+1);
             accessDetails.setAccesTimeStamps(System.currentTimeMillis());


            priorityQueue.putIfAbsent(valueRecord.getAccessDetails(), new ArrayList<>());
            priorityQueue.get(valueRecord.getAccessDetails()).add(valueRecord);
            return valueRecord.getValue();
         });

    }

    public CompletableFuture<Void> set(KEY key, VALUE value){
        Record<VALUE> valueRecord = new Record<>(value);
        if(!map.containsKey(key) && map.size() >= THRESHHOLD_SIZE) {
            if (evictionAlgorithm.equals(EvictionAlgorithm.LRU)) {

            } else {

            }
        }

        if(persistanceAlgorithm.equals(PersistanceAlgorithm.WRITE_THROUGH)){
            dataSource.persist(key, value).thenAccept(__-> map.put(key, valueRecord) );
        }else{
            addToCache(key, value);
            dataSource.persist(key, value);
            CompletableFuture.completedFuture(null);
        }
        addToCache(key, value);

        return CompletableFuture.completedFuture(null);
    }

    private Record<VALUE> addToCache(KEY key, VALUE value) {
        Record<VALUE> valueRecord = new Record<>(value);
        Long timeStamp = System.currentTimeMillis();
        expiryQueue.putIfAbsent(timeStamp, new CopyOnWriteArrayList<>());
        expiryQueue.get(timeStamp).add(valueRecord);
        map.put(key, valueRecord);
        return map.get(key);
    }
}

