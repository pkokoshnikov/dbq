package org.pak.dbq.internal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.pak.dbq.api.QueueName;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TestMessage  {
    public static QueueName QUEUE_NAME = new QueueName("test-message");

    private String name;
}
