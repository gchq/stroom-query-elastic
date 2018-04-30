package stroom.akka;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.util.ByteString;

import java.nio.file.Paths;
import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import akka.stream.*;
import akka.stream.javadsl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryStreams {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryStreams.class);

    public static void main(final String[] args) {
        final ActorSystem system = ActorSystem.create("QuickStartStreams");
        final Materializer materializer = ActorMaterializer.create(system);

        final Source<Integer, NotUsed> source = Source.range(1, 100);

//        LOGGER.info("Printing Numbers in Range");
//        final CompletionStage<Done> done =
//                source.runForeach(i -> System.out.println(i), materializer);
//        done.thenRun(system::terminate);

        final Source<BigInteger, NotUsed> factorials =
                source.scan(BigInteger.ONE, (acc, next) -> acc.multiply(BigInteger.valueOf(next)));

        LOGGER.info("Printing Factorials");
//        final CompletionStage<IOResult> result1 =
//                factorials
//                        .map(num -> ByteString.fromString(num.toString() + "\n"))
//                        .runWith(FileIO.toPath(Paths.get("factorial.txt")), materializer);
//
//        final CompletionStage<IOResult> result2 =
//                factorials.map(BigInteger::toString).runWith(lineSink("factorial2.txt"), materializer);

        final CompletionStage<Done> result3 = factorials
                .zipWith(Source.range(0, 99), (num, idx) -> String.format("%d! = %s", idx, num))
                .throttle(5, Duration.ofSeconds(1))
                .runForeach(System.out::println, materializer);

        result3.thenRun(system::terminate);
    }

    private static Sink<String, CompletionStage<IOResult>> lineSink(final String filename) {
        return Flow.of(String.class)
                .map(s -> ByteString.fromString(s + "\n"))
                .toMat(FileIO.toPath(Paths.get(filename)), Keep.right());
    }
}
