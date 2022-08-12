package org.kin.redis.bloomfilter;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.dynamic.RedisCommandFactory;
import org.kin.framework.utils.AbstractBloomFilter;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * 基于redis bitmap实现的bloom filter
 *
 * @author huangjianqin
 * @date 2022/3/4
 * @see com.google.common.hash.BloomFilter
 */
public final class RedisBloomFilter<T> extends AbstractBloomFilter<T> {
    /** 批量bit操作 */
    private BatchBitCommands commands;
    /** redis Key */
    private String key;

    public RedisBloomFilter(int predictElementSize, String key, StatefulConnection<?, ?> connection) {
        super(predictElementSize);
        init(key, connection);
    }

    public RedisBloomFilter(int predictElementSize, double fpp, String key, StatefulConnection<?, ?> connection) {
        super(predictElementSize, fpp);
        init(key, connection);
    }

    /**
     * @param predictElementSize 预估数据量
     * @param fpp                误判率
     * @param key                redis键
     */
    public RedisBloomFilter(int predictElementSize, double fpp, Function<T, byte[]> mapper, String key, StatefulConnection<?, ?> connection) {
        super(predictElementSize, fpp, mapper);
        init(key, connection);
    }

    /**
     * 初始化
     */
    private void init(String key, StatefulConnection<?, ?> connection) {
        this.key = key;
        RedisCommandFactory factory = new RedisCommandFactory(connection, Arrays.asList(ByteArrayCodec.INSTANCE, ByteArrayCodec.INSTANCE));
        commands = factory.getCommands(BatchBitCommands.class);
    }

    @Override
    protected void put(long[] indices) {
        for (long index : indices) {
            commands.setbit(key, index, 1);
        }
        commands.flush();
    }

    @Override
    protected boolean contains(long[] indices) {
        try {
            return containsAsync(indices).toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }

    /**
     * 检查元素在集合中是否存在(基于bloom filter的特性, 可能误判), 如果超时则返回false
     *
     * @param object    目标对象
     * @param timeoutMs 等待超时时间
     */
    public boolean contains(T object, int timeoutMs) {
        try {
            return containsAsync(getBitIndices(object)).toCompletableFuture().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return false;
        }
    }

    /**
     * 检查元素在集合中是否存在(基于bloom filter的特性, 可能误判)
     */
    public CompletionStage<Boolean> containsAsync(long[] indices) {
        CompletionStage<Boolean> ret = null;
        for (long index : indices) {
            CompletionStage<Boolean> stage = commands.getbit(key, index).thenApply(r -> r == 1);
            if (Objects.isNull(ret)) {
                ret = stage;
            } else {
                ret = ret.thenCombine(stage, (r1, r2) -> r1 && r2);
            }
        }
        commands.flush();
        return ret;
    }

    //getter
    public String getKey() {
        return key;
    }
}
