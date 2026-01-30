package com.machinecoding.CacheImplementation;

import java.util.concurrent.CompletableFuture;

public class DataSource<KEY, VALUE> {

    public CompletableFuture<VALUE> get(KEY key){
        return null;
    }

    public CompletableFuture<Void> persist(KEY key, VALUE value){
        return null;
    }
}
