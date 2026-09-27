package com.gsb.cache;

/**
 * 写模式：
 * WRITE_THROUGH —— put 时同步写穿到底层存储；
 * WRITE_BEHIND —— put 先进内存队列，由后台线程异步合并刷写。
 */
public enum WriteMode {
    WRITE_THROUGH,
    WRITE_BEHIND
}
