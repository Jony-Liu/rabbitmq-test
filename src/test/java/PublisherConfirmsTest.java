/**
 * @author Jony-Liu
 * @date 2019/11/25 13:35
 */

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests to illustrate different ways to handle publisher confirms.
 */
public class PublisherConfirmsTest {

    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter meter = metrics.meter("outbound-messages");
    Connection connection;
    String queue;
    int messageCount = 10_000;

    static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited = +100;
        }
        return condition.getAsBoolean();
    }

    @BeforeEach
    void init() throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost("localhost");
        cf.setUsername("guest");
        cf.setPassword("guest");
        connection = cf.newConnection();
        queue = UUID.randomUUID().toString();
        try (Channel ch = connection.createChannel()) {
            ch.queueDeclare(queue, false, false, true, null);
        }
    }

    @AfterEach
    void tearDown(TestInfo testInfo) throws Exception {
        System.out.println(String.format("%s: %.0f messages/second", testInfo.getDisplayName(), meter.getMeanRate()));
        connection.close(10_000);
    }

    @Test
    @DisplayName("publish messages individually")
    void publishMessagesIndividually() throws Exception {
        Channel ch = connection.createChannel();
        ch.confirmSelect();
        for (int i = 0; i < messageCount; i++) {
            String body = String.valueOf(i);
            ch.basicPublish("", queue, null, body.getBytes());
            ch.waitForConfirmsOrDie(5_000);
            meter.mark();
        }
        ch.close();

        CountDownLatch latch = new CountDownLatch(messageCount);
        ch = connection.createChannel();
        ch.basicConsume(queue, true, ((consumerTag, message) -> latch.countDown()), consumerTag -> {
        });

        assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    }
}