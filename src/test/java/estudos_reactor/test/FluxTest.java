package estudos_reactor.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
public class FluxTest {

    @BeforeAll
    public static void setUp(){
        BlockHound.install();
    }

    @Test
    public void fluxSubscriber(){
        Flux<String> fluxString = Flux.just("Jones", "Andreza", "Conrado").log();

        StepVerifier.create(fluxString).expectNext("Jones", "Andreza", "Conrado").verifyComplete();
    }

    @Test
    public void fluxSubscriberNumber(){
        Flux<Integer> fluxString = Flux.range(1,5).log();
        fluxString.subscribe(s -> log.info("number {}", s));

        StepVerifier.create(fluxString).expectNext(1,2,3,4,5).verifyComplete();
    }

    @Test
    public void fluxSubscriberFromList(){
        Flux<Integer> flux = Flux.fromIterable(List.of(1,2,3,4,5)).log();
        flux.subscribe(s -> log.info("number {}", s));

        StepVerifier.create(flux).expectNext(1,2,3,4,5).verifyComplete();
    }

    @Test
    public void fluxSubscriberNumberErrors(){
        Flux<Integer> flux = Flux.fromIterable(List.of(1,2,3,4,5)).log()
                .map( i -> {
                    if(i == 4){
                        throw new IndexOutOfBoundsException("index error");
                    }
                    return i;
                });

        flux.subscribe(
                s -> log.info("number {}", s),
                Throwable::printStackTrace,
                () -> log.info("DONE!"),
                subscription -> subscription.request(3));

        StepVerifier.create(flux)
                .expectNext(1,2,3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    public void fluxSubscriberUglyBackPressure(){
        Flux<Integer> flux = Flux.range(1,10).log();

        flux.subscribe(new Subscriber<Integer>() {

            private int count = 0;
            private Subscription subscription;
            private int requestCount = 2;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                this.subscription.request(2);
            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if(count >= 2){
                    count = 0;
                    subscription.request(2);
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });

        StepVerifier.create(flux)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .verifyComplete();
//                .expectError(IndexOutOfBoundsException.class)
//                .verify();
    }

    @Test
    public void fluxSubscriberNotSoUglyBackPressure(){
        Flux<Integer> flux = Flux.range(1,10).log();

        flux.subscribe(new BaseSubscriber<Integer>() {
            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if(count >= 2){
                    count = 0;
                    request(requestCount);
                }
            }

        });

        StepVerifier.create(flux)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
//                .expectError(IndexOutOfBoundsException.class)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberPrettyBackpressure(){
        Flux<Integer> fluxString = Flux.range(1,10).log().limitRate(3);
        fluxString.subscribe(s -> log.info("number {}", s));

        StepVerifier.create(fluxString).expectNext(1,2,3,4,5,6,7,8,9,10).verifyComplete();
    }

    @Test
    public void fluxSubscriberIntervalOne() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100)).take(10).log();
        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(3000);
    }

    @Test
    public void fluxSubscriberIntervalTwo() throws InterruptedException{
        StepVerifier.withVirtualTime(this::createInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofDays(1))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    private Flux<Long> createInterval() {
        return Flux.interval(Duration.ofDays(1)).log();
    }

    @Test
    public void connectableFLux() throws Exception{
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
//                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        connectableFlux.connect();

        Thread.sleep(300);

        connectableFlux.subscribe(i -> log.info("Sub1 number {}", i));

        Thread.sleep(200);

        connectableFlux.subscribe(i -> log.info("Sub2 number {}", i));

        StepVerifier.create(connectableFlux)
                .then(connectableFlux::connect)
                .thenConsumeWhile(i -> i <= 5)
                .expectNext(6,7,8,9,10)
                .expectComplete()
                .verify();

    }

    @Test
    public void connectableFLuxAutoConnect() {
        Flux<Integer> fluxAutoConnect = Flux.range(1, 5)
//                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(2);

        StepVerifier
                .create(fluxAutoConnect)
                .then(fluxAutoConnect::subscribe)
                .expectNext(1,2,3,4,5)
                .expectComplete()
                .verify();

    }
}
