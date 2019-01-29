/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 《令牌桶算法》
 *
 * Rate limiter implementation is based on token bucket algorithm. There are two parameters:
 * <ul>
 * <li>
 *     burst size - maximum number of requests allowed into the system as a burst
 * </li>
 * <li>
 *     average rate - expected number of requests per second (RateLimiters using MINUTES is also supported)
 * </li>
 * </ul>
 *
 * @author Tomasz Bak
 */
public class RateLimiter {

    private final long rateToMsConversion;

    /**
     * 令牌桶， 值表示的是 已被使用的令牌数量
     */
    private final AtomicInteger consumedTokens = new AtomicInteger();

    /**
     * 上次填充令牌桶的时间
     */
    private final AtomicLong lastRefillTime = new AtomicLong(0);

    @Deprecated
    public RateLimiter() {
        this(TimeUnit.SECONDS);
    }

    public RateLimiter(TimeUnit averageRateUnit) {
        switch (averageRateUnit) {
            case SECONDS:
                rateToMsConversion = 1000;
                break;
            case MINUTES:
                rateToMsConversion = 60 * 1000;
                break;
            default:
                throw new IllegalArgumentException("TimeUnit of " + averageRateUnit + " is not supported");
        }
    }

    /**
     *
     * @param burstSize 最大允许的请求数
     * @param averageRate 平均请求频率
     * @return
     */
    public boolean acquire(int burstSize, long averageRate) {
        return acquire(burstSize, averageRate, System.currentTimeMillis());
    }

    public boolean acquire(int burstSize, long averageRate, long currentTimeMillis) {
        if (burstSize <= 0 || averageRate <= 0) { // Instead of throwing exception, we just let all the traffic go
            return true;
        }

        refillToken(burstSize, averageRate, currentTimeMillis);
        return consumeToken(burstSize);
    }

    /**
     * 往令牌桶里边填充令牌
     * @param burstSize
     * @param averageRate
     * @param currentTimeMillis
     */
    private void refillToken(int burstSize, long averageRate, long currentTimeMillis) {
        long refillTime = lastRefillTime.get(); // 上次填充的时间
        long timeDelta = currentTimeMillis - refillTime;

        // 根据请求的平均频率计算每次新填充的令牌数量
        long newTokens = timeDelta * averageRate / rateToMsConversion;

        // 如果要填充的新令牌数量大于0（int整型的计算） 才去填充令牌，小于0的话就表示请求频率过高了 会限制
        if (newTokens > 0) {
            // 更新填充时间
            long newRefillTime = refillTime == 0
                    ? currentTimeMillis
                    // 这个直接 refillTime + timeDelta 就可以了。。。  秀自己的数学么
                    : refillTime + newTokens * rateToMsConversion / averageRate;

            // cas 尝试更新 lastRefillTime ， 如果失败就是其他线程已经更新了
            if (lastRefillTime.compareAndSet(refillTime, newRefillTime)) {
                while (true) {
                    // 更新已使用的令牌数量，用新令牌数去抵消
                    int currentLevel = consumedTokens.get();
                    int adjustedLevel = Math.min(currentLevel, burstSize); // In case burstSize decreased
                    int newLevel = (int) Math.max(0, adjustedLevel - newTokens);
                    if (consumedTokens.compareAndSet(currentLevel, newLevel)) {
                        return;
                    }
                }
            }
        }
    }

    /**
     * 尝试获取令牌
     * @param burstSize
     * @return
     */
    private boolean consumeToken(int burstSize) {
        while (true) {
            int currentLevel = consumedTokens.get();
            // 如果当前已消费的令牌大于最大允许进入系统的请求数量，则直接失败
            if (currentLevel >= burstSize) {
                return false;
            }
            if (consumedTokens.compareAndSet(currentLevel, currentLevel + 1)) {
                return true;
            }
        }
    }

    public void reset() {
        consumedTokens.set(0);
        lastRefillTime.set(0);
    }
}
