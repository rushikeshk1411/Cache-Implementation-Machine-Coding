package com.machinecoding.CacheImplementation.cache;

import com.machinecoding.CacheImplementation.DataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;

public class Cache<KEY, VALUE> {

    private final Map<KEY, Record<VALUE>> map;
    private final DataSource<KEY, VALUE> dataSource;
    private final PersistanceAlgorithm persistanceAlgorithm;
    private final EvictionAlgorithm evictionAlgorithm;
    private final Integer expirationTimeInMilliSecond;
    private final Map<Long, List<Record<VALUE>>> expiryQueue;
    private final Map<AccessDetails, List<Record<VALUE>>> priorityQueue;
    private final ExecutorService threadPool[];
    private static final Integer THRESHHOLD_SIZE = 500;
    static final int poolSize = 5;


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

        this.threadPool = new ExecutorService[5];

        for(int i = 0; i< poolSize; i++){
            threadPool[i] = Executors.newSingleThreadExecutor();
        }
    }

    public CompletableFuture<VALUE> get(KEY key){
        return CompletableFuture.supplyAsync(() -> getInAssignedThread(key), threadPool[key.hashCode()%poolSize])
                .thenCompose(Function.identity());
    }

    private CompletableFuture<VALUE> getInAssignedThread(KEY key) {
        final CompletableFuture<Record<VALUE>> result;
        Record<VALUE> record = map.get(key);
        if(map.containsKey(key) && record.getAccessDetails().getAccesTimeStamps() >= System.currentTimeMillis() - expirationTimeInMilliSecond){
            result = CompletableFuture.completedFuture(record);
        }else{
            expiryQueue.get(record.getAccessDetails().getAccesTimeStamps()).remove(record);
            priorityQueue.get(record.getAccessDetails()).remove(record);
            result = dataSource.get(key).thenApply(value -> addToCache(key, value));
        }

        return result.thenApply((valueRecord) -> {
            final var accessDetails = valueRecord.getAccessDetails();
            accessDetails.setAccessCount(accessDetails.getAccessCount() + 1);
            accessDetails.setAccesTimeStamps(System.currentTimeMillis());

            priorityQueue.putIfAbsent(valueRecord.getAccessDetails(), new ArrayList<>());
            priorityQueue.get(valueRecord.getAccessDetails()).add(valueRecord);
            return valueRecord.getValue();
        });
    }

    public CompletableFuture<Void> set(KEY key, VALUE value){
        return CompletableFuture.supplyAsync(() -> setInAssignedThread(key, value), threadPool[key.hashCode()%poolSize])
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Void> setInAssignedThread(KEY key, VALUE value) {
        if(map.containsKey(key)){
            Record<VALUE>record = map.remove(key);
            expiryQueue.remove(record.getLoadTime());
            priorityQueue.remove(record.getAccessDetails());
        }
        Record<VALUE> valueRecord = new Record<>(value);
        if(map.size() >= THRESHHOLD_SIZE) {
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

