package com.machinecoding.CacheImplementation.cache;

public class Record<VALUE> implements Comparable<Record<VALUE>>{

    private final VALUE value;
    private long loadTime;
    private AccessDetails accessDetails;

    public Record(VALUE value){
        this.value = value;
    }

    @Override
    public int compareTo(Record value) {
        return (int) (accessDetails.getAccesTimeStamps() - value.accessDetails.getAccesTimeStamps());
    }

    public VALUE getValue() {
        return value;
    }

    public AccessDetails getAccessDetails(){
        return accessDetails;
    }




}


