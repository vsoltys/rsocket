package com.memphis.sandbox.rsocket.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.rsocket.RSocketSecurity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.messaging.handler.invocation.reactive.AuthenticationPrincipalArgumentResolver;
import org.springframework.security.rsocket.core.PayloadSocketAcceptorInterceptor;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

@SpringBootApplication
public class ServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServiceApplication.class, args);
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
class GreetingController {

    @MessageMapping("greetings")
    Flux<GreetingResponse> greet(@AuthenticationPrincipal Mono<UserDetails> user, RSocketRequester connection) {
        return user.map(UserDetails::getUsername)
                .map(GreetingRequest::new)
                .flatMapMany(request -> this.greet(request, connection));
    }

    Flux<GreetingResponse> greet(GreetingRequest request, RSocketRequester connection) {

        var in = connection.route("health")
                .retrieveFlux(ClientHealthState.class)
                .filter(clientHealthState -> !clientHealthState.isHealthy());

        var out = Flux.fromStream(Stream.generate(() -> new GreetingResponse("ni hao " + request.getName() + " @ " + Instant.now() + "!")))
                .take(100)
                .delayElements(Duration.ofSeconds(1));

        return out.takeUntilOther(in);
    }

}

@Configuration
class SecurityConfiguration {

    @Bean
    PayloadSocketAcceptorInterceptor authorization(RSocketSecurity security) {
        return security
                .authorizePayload(spec -> spec.anyExchange().authenticated())
                .simpleAuthentication(Customizer.withDefaults())
//				.jwt(...)
                .build();
    }

    @Bean
    MapReactiveUserDetailsService authentication() {
        var user = User.withDefaultPasswordEncoder()
                .username("memphis")
                .password("pwd")
                .roles("USER")
                .build();

        return new MapReactiveUserDetailsService(user);
    }

    @Bean
    RSocketMessageHandler messageHandler(RSocketStrategies strategies) {
        // this one is for Flux<GreetingResponse> greet(@AuthenticationPrincipal... to work
        var handler = new RSocketMessageHandler();
        handler.getArgumentResolverConfigurer().addCustomResolver(new AuthenticationPrincipalArgumentResolver());
        handler.setRSocketStrategies(strategies);

        return handler;
    }
}