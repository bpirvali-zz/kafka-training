package com.bp.samples.util;

import com.sun.istack.NotNull;

/**
 * Created by behzad.pirvali on 1/7/17.
 */
public interface KafkaHelper {
    public void sendString(@NotNull String topic, @NotNull String key, @NotNull String value);
    public void close();
}
