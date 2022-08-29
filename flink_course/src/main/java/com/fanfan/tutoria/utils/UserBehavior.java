package com.fanfan.tutoria.utils;

/**
 * @ClassName: UserBehavior
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月29日 11时47分
 * @Version: v1.0
 */
public class UserBehavior {
    public String userId;
    public String productId;
    public String categoryId;
    public String type;
    public Long ts;

    public UserBehavior(String userId, String productId, String categoryId, String type, Long ts) {
        this.userId = userId;
        this.productId = productId;
        this.categoryId = categoryId;
        this.type = type;
        this.ts = ts;
    }

    public UserBehavior() {
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + ts +
                '}';
    }
}
