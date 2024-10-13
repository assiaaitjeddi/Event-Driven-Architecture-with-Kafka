package ma.enset.demospringcloudstreamskafka.services;

import ma.enset.demospringcloudstreamskafka.entities.PageEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class PageEventService {
    //pour lire les messages(exploiter la programmation fonctionnel)
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (pageEvent -> {
            System.out.println("***********************");
            System.out.println(pageEvent.toString());
            System.out.println("***********************");
        });
    }

    @Bean
    //pour publier la fonction
    public Supplier<PageEvent> pageEventSupplier(){
        return ()->new PageEvent(
        Math.random()>0.5?"P1":"P2",
        Math.random()>0.5?"U1":"U2",
        new Date(),
        new Random().nextLong(9000));
    }

    @Bean
    //une fct qui recoit un flux en entrée et fait le traitement et aprés il va produire un flux en sortit
    public Function<PageEvent, PageEvent> pageEventFunction(){
        return (input)->{
            input.setName("L : "+input.getName().length());
            input.setUser("UUUUUUU");
            return input;
        };
    }


    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {
        return (input) -> input
                .filter((k, v) -> v.getDuration() > 100)
                .map((k, v) -> new KeyValue<>(v.getName(), 0L))
                .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(5000)))
                .count(Materialized.as("page-count"))
                .toStream()
                .map((k, v) -> new KeyValue<>("=>" + k.window().startTime() + k.window().endTime() + k.key(), v));
    }



}
