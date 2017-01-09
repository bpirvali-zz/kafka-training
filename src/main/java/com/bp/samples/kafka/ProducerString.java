package com.bp.samples.kafka;

import com.sun.istack.NotNull;

/**
 * Created by behzad.pirvali on 1/8/17.
 */
public interface ProducerString {
    public void send(@NotNull String topic, @NotNull String key, @NotNull String value);
    public void close();
}
