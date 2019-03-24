/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.distribution.journal.kafka;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.sling.distribution.journal.JsonMessageSender;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.kafka.util.KafkaRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

/**
 * Non OSGi test for the interaction of JsonMessageSender, JsonMessagePoller
 */
public class JsonMessagingTest {

    public static class Person {
        public String name;
    }

    private static final String TOPIC_NAME = "test";
    
    private Semaphore sem = new Semaphore(0);
    private Person lastMessage;
    
    @ClassRule
    public static KafkaRule kafka = new KafkaRule();
    
    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }
    
    @Test
    public void testSendReceive() throws InterruptedException, IOException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
        MessagingProvider provider = kafka.getProvider();
        Person msg = new Person();
        msg.name = "Joe";
        Closeable poller = provider.createJsonPoller(TOPIC_NAME, Reset.earliest, this::handle, Person.class);
        JsonMessageSender<Person> messageSender = provider.createJsonSender();
        
        messageSender.send(TOPIC_NAME, msg);
        assertReceived();
        assertThat(this.lastMessage, samePropertyValuesAs(msg));
        poller.close();
    }
    
    private void assertReceived() throws InterruptedException {
        assertTrue(sem.tryAcquire(30, TimeUnit.SECONDS));
    }
    
    private void handle(MessageInfo info, Person message) {
        this.lastMessage = message;
        this.sem.release();
    }
    
}
