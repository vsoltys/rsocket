package com.memphis.sandbox.rsocket.client;

import io.rsocket.SocketAcceptor;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.rsocket.messaging.RSocketStrategiesCustomizer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.rsocket.metadata.SimpleAuthenticationEncoder;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.stream.Stream;

@SpringBootApplication
public class ClientApplication {

    // prepare auth metadata for rsocket connection
    private final UsernamePasswordMetadata credentials = new UsernamePasswordMetadata("memphis", "pwd");
    private final MimeType mimeType = MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());

    @SneakyThrows
    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
        System.in.read();
    }

    @Bean
    SocketAcceptor socketAcceptor(RSocketStrategies strategies, HealthController healthController) {
        return RSocketMessageHandler.responder(strategies, healthController);
    }

    @Bean
    RSocketRequester rSocketRequester(RSocketRequester.Builder builder, SocketAcceptor socketAcceptor) {
        return builder
                .setupMetadata(this.credentials, this.mimeType)
                .rsocketConnector(connector -> connector.acceptor(socketAcceptor))
                .tcp("localhost", 8888);
    }

    @Bean
    RSocketStrategiesCustomizer rSocketStrategiesCustomizer() {
        return strategies -> strategies.encoder(new SimpleAuthenticationEncoder());
    }

    @Bean
    ApplicationListener<ApplicationReadyEvent> client(RSocketRequester client) {
        return event -> {
            var greetingResponseFlux = client.route("greetings")
//                    .metadata(this.credentials, this.mimeType) either directly here in request or configure in requester bean
//                    .data(new GreetingRequest("operator"))    not used in authenticated request
                    .data(Mono.empty())
                    .retrieveFlux(GreetingResponse.class);

            greetingResponseFlux.subscribe(System.out::println);
        };
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class GreetingRequest {
    private String name;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class GreetingResponse {
    private String message;
}


@Data
@AllArgsConstructor
@NoArgsConstructor
class ClientHealthState {
    private boolean healthy;
}

@Controller
class HealthController {

    @MessageMapping("health")
    Flux<ClientHealthState> health() {
        var stream = Stream.generate(() -> new ClientHealthState(Math.random() > .2));

        return Flux.fromStream(stream)
                .take(100)
                .delayElements(Duration.ofSeconds(1));
    }
}