package com.machinecoding.CacheImplementation.cache;

public class AccessDetails {
    private Long accessCount;
    private Long accesTimeStamps;


    public AccessDetails(Long accessCount, Long accesTimeStamps){
        this.accessCount = accessCount;
        this.accesTimeStamps = accesTimeStamps;
    }

    public Long getAccessCount(){
        return accessCount;
    }

    public Long getAccesTimeStamps(){
        return accesTimeStamps;
    }

    public void setAccessCount(Long accessCount){
        this.accessCount = accessCount;
    }

    public void setAccesTimeStamps(Long accesTimeStamps){
        this.accesTimeStamps = accesTimeStamps;
    }


}
