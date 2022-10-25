package estudos_reactor.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
public class MonoTest {

    @BeforeAll
    public static void setUp(){
        BlockHound.install();
    }

    @Test
    public void monoSubscriber(){
        String name = "Jones";
        Mono<String> mono = Mono.just(name).log();

        StepVerifier.create(mono).expectNext(name).verifyComplete();
        log.info("Everything working as intended");
    }

    @Test
    public void monoSubscriberConsumer(){
        String name = "Jones";
        Mono<String> mono = Mono.just(name).log();

        mono.subscribe(s -> log.info("value {}", s));

        StepVerifier.create(mono).expectNext(name).verifyComplete();
        log.info("Everything working as intended");
    }

    @Test
    public void monoSubscriberConsumerError(){
        String name = "Jones";
        Mono<String> mono = Mono.just(name).map(s -> { throw new RuntimeException("Testing mono with error");});

        mono.subscribe(
                s -> log.info("value {}", s),
                s -> log.error("Something bad happened"));

        mono.subscribe(
                s -> log.info("value {}", s),
                Throwable::printStackTrace);

        StepVerifier.create(mono).expectError(RuntimeException.class).verify();
    }

    @Test
    public void monoSubscriberConsumerComplete(){
        String name = "Jones";
        Mono<String> mono = Mono.just(name).log().map(String::toUpperCase);

        mono.subscribe(
                s -> log.info("value {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!!"));

        StepVerifier.create(mono).expectNext(name.toUpperCase()).verifyComplete();
        log.info("Everything working as intended");
    }

    @Test
    public void monoSubscriberConsumerSubcription(){
        String name = "Jones";
        Mono<String> mono = Mono.just(name).log().map(String::toUpperCase);

        mono.subscribe(
                s -> log.info("value {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!!"),
                subscription -> subscription.request(5));

//        StepVerifier.create(mono).expectNext(name.toUpperCase()).verifyComplete();
    }

    @Test
    public void monoDoOnMethods(){
        String name = "Jones";
        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Subscribed"))
                .doOnRequest(longNumber -> log.info("Request received"))
                .doOnNext(s -> log.info("value  doOnNext {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("value  doOnNext {}", s))
                .doOnSuccess(s -> log.info("doONsuccess"));

        mono.subscribe(
                s -> log.info("value {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!!"));

//        StepVerifier.create(mono).expectNext(name.toUpperCase()).verifyComplete();
        log.info("Everything working as intended");
    }

    @Test
    public void monoDoOnError(){
        Mono<Object> error = Mono.error(new IllegalArgumentException("mensagem de erro"))
                        .doOnError(e -> log.error("Error message: {}",e.getMessage()))
                        .log();

        StepVerifier.create(error).expectError(IllegalArgumentException.class).verify();
    }

    @Test
    public void monoDoOnErrorResume(){
        String name = "Jones";
        Mono<Object> error = Mono.error(new IllegalArgumentException("mensagem de erro"))
                .onErrorResume(s -> {
                    log.info("errorresume");
                    return Mono.just(name);
                })
                .doOnError(e -> log.error("Error message: {}",e.getMessage()))
                .log();

        StepVerifier.create(error).expectNext(name).verifyComplete();
    }

    @Test
    public void monoDoOnErrorReturn(){
        String name = "Jones";
        Mono<Object> error = Mono.error(new IllegalArgumentException("mensagem de erro"))
                .onErrorReturn("EMPTY")
                .onErrorResume(s -> {
                    log.info("errorresume");
                    return Mono.just(name);
                })
                .doOnError(e -> log.error("Error message: {}",e.getMessage()))
                .log();

        StepVerifier.create(error).expectNext("EMPTY").verifyComplete();
    }
}
