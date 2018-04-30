package stroom.akka;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TweetStreams {
    public static void main(String[] args) {
        // System
        final ActorSystem system = ActorSystem.create("QuickStartStreams");
        final Materializer materializer = ActorMaterializer.create(system);

        // Create source
        final Source<Tweet, NotUsed> tweets =
                Source.from(Arrays.asList(
                        new Tweet(new Author("joe"), System.currentTimeMillis(), "some stuff in #akka is way #complicado"),
                        new Tweet(new Author("kate"), System.currentTimeMillis(), "so #tired"),
                        new Tweet(new Author("tomS"), System.currentTimeMillis(), "i could write my own version of akka"),
                        new Tweet(new Author("indigo"), System.currentTimeMillis(), "i'm too young to understand #akka"),
                        new Tweet(new Author("sue"), System.currentTimeMillis(), "crafty #sparkles and #akka goodness"),
                        new Tweet(new Author("sue"), System.currentTimeMillis(), "stuff on my #cat"),
                        new Tweet(new Author("jamie"), System.currentTimeMillis(), "i sell cars #akka"),
                        new Tweet(new Author("tomH"), System.currentTimeMillis(), "i #work way hard")
                ));

        final Source<Tweet, NotUsed> tweetsStateful =
                Source.unfold(5, (i) -> {
                    if (i > 0) {
                        return Optional.of(Pair.create(i - 1, new Tweet(new Author("unfolder"), System.currentTimeMillis(), "content" + i)));
                    } else {
                        return Optional.empty();
                    }
                });

        //tweetsStateful.runForeach(System.out::println, materializer);

        final Source<Author, NotUsed> authors =
                tweets
                        .filter(t -> t.hashtags().contains(AKKA))
                        .map(t -> t.author);

        tweets.groupBy(7, t -> t.author)
                .map(t -> new Pair<>(t.author, t.hashtags()))
                .reduce((left, right) -> {
                    HashSet<Hashtag> s = new HashSet<>();
                    s.addAll(left.second());
                    s.addAll(right.second());
                    return new Pair<>(left.first(), s);
                })
                .mergeSubstreams()
                .runForeach(System.out::println, materializer)
                .exceptionally(e -> {
                    e.printStackTrace();
                    return Done.getInstance();
                });


//        final CompletionStage<Done> result1 =
//                authors.runWith(Sink.foreach(System.out::println), materializer);
//
//        final Source<Hashtag, NotUsed> hashtags =
//                tweets.mapConcat(t -> new ArrayList<>(t.hashtags()));
//
//        final CompletionStage<Done> result2 =
//                hashtags.runWith(Sink.foreach(System.out::println), materializer);

        final Sink<Integer, CompletionStage<Integer>> sumSink =
                Sink.fold(0, (acc, elem) -> acc + elem);

        final RunnableGraph<CompletionStage<Integer>> counter =
                tweets.map(t -> 1).toMat(sumSink, Keep.right());

        final CompletionStage<Integer> sum = counter.run(materializer);

        sum.thenAcceptAsync(c -> System.out.println("Total tweets processed: " + c),
                system.dispatcher());

        sum.thenRun(system::terminate);
    }

    public static class Author {
        public final String handle;

        public Author(String handle) {
            this.handle = handle;
        }

        // ...

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Author{");
            sb.append("handle='").append(handle).append('\'');
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Author author = (Author) o;
            return Objects.equals(handle, author.handle);
        }

        @Override
        public int hashCode() {
            return Objects.hash(handle);
        }
    }

    public static class Hashtag {
        public final String name;

        public Hashtag(String name) {
            this.name = name;
        }

        // ...

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Hashtag{");
            sb.append("name='").append(name).append('\'');
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Hashtag hashtag = (Hashtag) o;
            return Objects.equals(name, hashtag.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Tweet {
        public final Author author;
        public final long timestamp;
        public final String body;

        public Tweet(Author author, long timestamp, String body) {
            this.author = author;
            this.timestamp = timestamp;
            this.body = body;
        }

        public Set<Hashtag> hashtags() {
            return Arrays.asList(body.split(" ")).stream()
                    .filter(a -> a.startsWith("#"))
                    .map(a -> new Hashtag(a))
                    .collect(Collectors.toSet());
        }

        // ...

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Tweet{");
            sb.append("author=").append(author);
            sb.append(", timestamp=").append(timestamp);
            sb.append(", body='").append(body).append('\'');
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tweet tweet = (Tweet) o;
            return timestamp == tweet.timestamp &&
                    Objects.equals(author, tweet.author) &&
                    Objects.equals(body, tweet.body);
        }

        @Override
        public int hashCode() {
            return Objects.hash(author, timestamp, body);
        }
    }

    public static final Hashtag AKKA = new Hashtag("#akka");
}
