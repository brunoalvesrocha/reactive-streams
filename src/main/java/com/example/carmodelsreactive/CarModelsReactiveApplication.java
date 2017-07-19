package com.example.carmodelsreactive;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.reactive.function.server.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;

@SpringBootApplication
public class CarModelsReactiveApplication {

    public static void main(String[] args) {
        SpringApplication.run(CarModelsReactiveApplication.class, args);
    }

    @Component
    public static class RouteHandles {
        private final FluxCarService fluxCarService;

        public RouteHandles(FluxCarService fluxCarService) {
            this.fluxCarService = fluxCarService;
        }

        public Mono<ServerResponse> allCars(ServerRequest serverRequest) {
            return ServerResponse.ok()
                    .body(fluxCarService.all(), Car.class)
                    .doOnError(throwable -> new IllegalStateException("My godness NOOOOO!!"));
        }

        public Mono<ServerResponse> carById(ServerRequest serverRequest) {
            String carId = serverRequest.pathVariable("carId");
            return ServerResponse.ok()
                    .body(fluxCarService.byId(carId), Car.class)
                    .doOnError(throwable -> new IllegalStateException("oh boy... not againnn =(("));
        }

        public Mono<ServerResponse> events(ServerRequest serverRequest) {
            String carId = serverRequest.pathVariable("carId");
            return ServerResponse.ok()
                    .contentType(MediaType.TEXT_EVENT_STREAM)
                    .body(fluxCarService.streams(carId), CarEvents.class)
                    .doOnError(throwable -> new IllegalStateException("I give up!! "));
        }
    }

    @Bean
    RouterFunction<?> routes(RouteHandles routeHandles) {
        return RouterFunctions.route(
                RequestPredicates.GET("/async/cars"), routeHandles::allCars)
                .andRoute(RequestPredicates.GET("/async/cars/{carId}"), routeHandles::carById)
                .andRoute(RequestPredicates.GET("/async/cars/{carId}/events"), routeHandles::events);
    }
}

//@RestController
class CarController {

    private final FluxCarService fluxCarService;

    CarController(FluxCarService fluxCarService) {
        this.fluxCarService = fluxCarService;
    }

    @GetMapping("/sync/cars")
    public Flux<Car> all() {
        return fluxCarService.all();
    }

    @GetMapping("/sync/cars/{carId}")
    public Mono<Car> byId(@PathVariable String carId) {
        return fluxCarService.byId(carId);
    }

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE, value = "/sync/cars/{carId}/events")
    public Flux<CarEvents> eventsOfStreams(@PathVariable String carId) {
        return fluxCarService.streams(carId);
    }
}

@Service
class FluxCarService {

    private final CarRepository carRepository;

    FluxCarService(CarRepository carRepository) {
        this.carRepository = carRepository;
    }

    public Flux<Car> all() {
        return carRepository.findAll();
    }

    public Mono<Car> byId(String carId) {
        return carRepository.findById(carId);
    }

    public Flux<CarEvents> streams(String carId) {
        return byId(carId).flatMapMany(car -> {
            Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
            Flux<CarEvents> events = Flux.fromStream(
                    Stream.generate(() -> new CarEvents(car, new Date())));
            return Flux.zip(interval, events).map(Tuple2::getT2);
        });
    }
}

@Component
class DummyData implements CommandLineRunner {

    private final CarRepository carRepository;

    DummyData(CarRepository carRepository) {
        this.carRepository = carRepository;
    }

    @Override
    public void run(String... args) throws Exception {

        carRepository.deleteAll()
                .thenMany(
                        Flux.just("Koenigsegg One:1", "Hennessy Venom GT", "Bugatti Veyron Super Sport",
                                "SSC Ultimate Aero", "McLaren F1", "Pagani Huayra", "Noble M600",
                                "Aston Martin One-77", "Ferrari LaFerrari", "Lamborghini Aventador")
                                .map(model -> new Car(UUID.randomUUID().toString(), model))
                                .flatMap(carRepository::save))
                .subscribe(System.out::println);

//        Disposable subscribe = Mono.fromCallable(() -> System.currentTimeMillis())
//                .repeat()
//                .parallel(2)
//                .runOn(Schedulers.parallel())
//                .doOnNext(d -> System.out.println("I'm on thread: " + Thread.currentThread()))
//                .sequential()
//                .subscribe();
    }
}

interface CarRepository extends ReactiveMongoRepository<Car, String> {
}

@Document
@Data
@AllArgsConstructor
@NoArgsConstructor
class Car {

    @Id
    private String id;
    private String model;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class CarEvents {

    private Car model;
    private Date when;
}
