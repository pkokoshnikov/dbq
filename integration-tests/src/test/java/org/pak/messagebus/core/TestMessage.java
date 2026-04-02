package org.pak.messagebus.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TestMessage  {
    public static QueueName QUEUE_NAME = new QueueName("test-message");

    private String name;
}
