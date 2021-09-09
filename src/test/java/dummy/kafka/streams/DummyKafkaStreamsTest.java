package dummy.kafka.streams;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.runtime.EmbeddedApplication;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import jakarta.inject.Inject;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@MicronautTest
class DummyKafkaStreamsTest {

    @Inject
    EmbeddedApplication<?> application;

    @Inject
    private ConfiguredStreamBuilder streamBuilder;

    private TopologyTestDriver testDriver;

    private TestOutputTopic<String, String> output;
    private TestInputTopic<String, String> input;
    private TestInputTopic<String, String> table;

    @BeforeEach
    public void beforeEach() {
        Topology topology = streamBuilder.build();

        final Properties configuration = streamBuilder.getConfiguration();
        configuration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configuration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configuration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(topology, configuration);

        output =
                testDriver.createOutputTopic(
                        "output",
                        Serdes.String().deserializer(),
                        Serdes.String().deserializer());

        input =
                testDriver.createInputTopic(
                        "input",
                        Serdes.String().serializer(),
                        Serdes.String().serializer());

        table =
                testDriver.createInputTopic(
                        "table",
                        Serdes.String().serializer(),
                        Serdes.String().serializer());
    }

    @AfterEach
    public void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testTestDriver() {
        input.pipeInput("key_1", "message_1");
        input.pipeInput("key_2", "message_2");
        input.pipeInput("key_3", "");

        final List<KeyValue<String, String>> keyValues = output.readKeyValuesToList();
        assertEquals(2, keyValues.size());
    }

}
