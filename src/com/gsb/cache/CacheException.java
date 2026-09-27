package com.gsb.cache;

/**
 * 缓存操作失败时抛出的运行时异常。
 */
public class CacheException extends RuntimeException {
    public CacheException(String message) {
        super(message);
    }

    public CacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
