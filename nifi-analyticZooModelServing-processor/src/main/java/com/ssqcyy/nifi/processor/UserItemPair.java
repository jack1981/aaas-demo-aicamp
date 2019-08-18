package com.ssqcyy.nifi.processor;
/**
 * @author suqiang.song
 *
 */
public class UserItemPair {

    private final Float userId;
    private final Float itemId;

    public UserItemPair(Float userId, Float itemId){

        this.userId = userId;
        this.itemId = itemId;
    }

    public Float getUserId() {
        return userId;
    }

    public Float getItemId() {
        return itemId;
    }
}