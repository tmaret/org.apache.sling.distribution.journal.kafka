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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.kafka.util.KafkaRule;
import org.apache.sling.distribution.journal.messages.Messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.Messages.SubscriberConfiguration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class MessagingTest {

    private String topicName;
    private Semaphore sem = new Semaphore(0);
    private volatile MessageInfo lastInfo;
    
    @ClassRule
    public static KafkaRule kafka = new KafkaRule();
    
    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        topicName = "MessagingTest" + UUID.randomUUID().toString();
    }
    
    @Test
    public void testSendReceive() throws Exception {
        MessagingProvider provider = kafka.getProvider();
        HandlerAdapter<DiscoveryMessage> handler = HandlerAdapter.create(DiscoveryMessage.class, this::handle);
        Closeable poller = provider.createPoller(topicName, Reset.earliest, handler);
        DiscoveryMessage msg = DiscoveryMessage.newBuilder()
                .setSubAgentName("sub1agent")
                .setSubSlingId("subsling")
                .setSubscriberConfiguration(SubscriberConfiguration
                        .newBuilder()
                        .setEditable(false)
                        .setMaxRetries(-1)
                        .build())
                .build();
        MessageSender<DiscoveryMessage> messageSender = provider.createSender();
        
        // After starting Kafka, sending and receiving should work
        messageSender.send(topicName, msg);
        assertReceived();

        poller.close();
    }
    
    @Test
    public void testAssign() throws Exception {
        MessagingProvider provider = kafka.getProvider();
        DiscoveryMessage msg = DiscoveryMessage.newBuilder()
                .setSubAgentName("sub1agent")
                .setSubSlingId("subsling")
                .setSubscriberConfiguration(SubscriberConfiguration
                        .newBuilder()
                        .setEditable(false)
                        .setMaxRetries(-1)
                        .build())
                .build();
        MessageSender<DiscoveryMessage> messageSender = provider.createSender();
        messageSender.send(topicName, msg);
        
        HandlerAdapter<DiscoveryMessage> handler = HandlerAdapter.create(DiscoveryMessage.class, this::handle);
        long offset;
        try (Closeable poller = provider.createPoller(topicName, Reset.earliest, handler)) {
            assertReceived();
            offset = lastInfo.getOffset();
        }
        
        // Starting from old offset .. should see our message
        String assign = "0:" + offset;
        try (Closeable poller = provider.createPoller(topicName, Reset.latest, assign, handler)) {
            assertReceived();
            assertThat(lastInfo.getOffset(), equalTo(offset));
        }
        
        // Starting from invalid offset. Should see old message as we start from earliest
        String invalid = "0:32532523453";
        try (Closeable poller = provider.createPoller(topicName, Reset.earliest, invalid, handler)) {
            assertReceived();
        }
        
        // Starting from invalid offset. Should not see any message as we start from latest
        try (Closeable poller = provider.createPoller(topicName, Reset.latest, invalid, handler)) {
            assertNotReceived();
        }
    }

    private void assertReceived() throws InterruptedException {
        assertTrue(sem.tryAcquire(30, TimeUnit.SECONDS));
    }
    
    private void assertNotReceived() throws InterruptedException {
        assertFalse(sem.tryAcquire(2, TimeUnit.SECONDS));
    }

    private void handle(MessageInfo info, DiscoveryMessage message) {
        this.lastInfo = info;
        this.sem.release();
    }
    
}
