package ru.wikimart;

import java.io.Serializable;


public class Task implements Serializable{
    private String resourceId;
    private boolean isExclusive;

    public Task(String resourceId, boolean isExclusive) {
        this.resourceId = resourceId;
        this.isExclusive = isExclusive;
    }

    public boolean isExclusive() {
        return isExclusive;
    }

    public void setExclusive(boolean isExclusive) {
        this.isExclusive = isExclusive;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    @Override
    public String toString() {
        return "Task{" +
                "resourceId='" + resourceId + '\'' +
                ", isExclusive=" + isExclusive +
                '}';
    }
}
