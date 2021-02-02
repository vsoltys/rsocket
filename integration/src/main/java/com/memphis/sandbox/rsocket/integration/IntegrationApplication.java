package com.memphis.sandbox.rsocket.integration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.codec.json.Jackson2JsonDecoder;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.rsocket.ClientRSocketConnector;
import org.springframework.integration.rsocket.RSocketInteractionModel;
import org.springframework.integration.rsocket.dsl.RSockets;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

import java.io.File;
import java.time.Duration;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SpringBootApplication
public class IntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(IntegrationApplication.class, args);
    }

    @Bean
    RSocketStrategies rSocketStrategies() {
        return RSocketStrategies.builder()
                .encoder(new Jackson2JsonEncoder())
                .decoder(new Jackson2JsonDecoder())
                .build();
    }

    @Bean
    ClientRSocketConnector clientRSocketConnector(RSocketStrategies strategies) {
        var clientConnector = new ClientRSocketConnector("localhost", 8888);
        clientConnector.setRSocketStrategies(strategies);

        return clientConnector;
    }

    @Bean
    MessageChannel reactiveMessageChannel() {
        return MessageChannels.flux().get();
    }

    @Bean
    IntegrationFlow rsocketFlow(ClientRSocketConnector connector, @Value("${user.home}") File home) {

        // file inbound adapter
        var folder = new File(new File(home, "Desktop"), "in");
        var fileReadingMessageSource = Files.inboundAdapter(folder)
                .autoCreateDirectory(true)
                .get();

        // rsocket outbound gateway
        var rsocket = RSockets.outboundGateway("greetings")
                .interactionModel(RSocketInteractionModel.requestStream)
                .clientRSocketConnector(connector)
                .expectedResponseType(GreetingResponse.class);

        // processing
        return IntegrationFlows.from(fileReadingMessageSource, pollerConfig -> pollerConfig.poller(pollerFactory -> pollerFactory.fixedRate(1_000)))
                .transform(new FileToStringTransformer())
                .transform(String.class, name -> new GreetingRequest(name.trim()))
                .handle(rsocket)
                .split()
                .channel(reactiveMessageChannel())
                .handle((GenericHandler<GreetingResponse>) (greetingResponse, headers) -> {
                    System.out.println("new message:" + greetingResponse.toString());
                    return null;
                })
                .get();
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