package com.example.kafkatwitterconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaTwitterConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaTwitterConsumerApplication.class, args);

		try {
			Pipeline.invoke();

			//Pipeline2.invoke();

		} catch (Exception e) {
			e.printStackTrace();
		}


	}

}
