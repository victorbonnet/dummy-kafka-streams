package dummy.kafka.streams;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.streams.kstream.Materialized;

@Factory
public class ExampleFactory {

    @Singleton
    KStream<String, String> exampleStream(ConfiguredStreamBuilder builder) {

        final KStream<String, String> input = builder.stream("input");

        final KStream<String, String> filter = input.filter((k, v) -> !v.isEmpty());

        // GIVES TIMEOUT
        final GlobalKTable<String, String> globalTable = builder.globalTable("table",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("global"));

        final KStream<String, String> join = filter.leftJoin(
                globalTable,
                (k, v) -> v,
                (l, r) -> l
        );
        join.to("output");
        // END: GIVES TIMEOUT

//        filter.to("output");

        return input;
    }
}
