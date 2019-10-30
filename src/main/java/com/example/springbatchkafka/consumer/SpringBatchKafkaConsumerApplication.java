package com.example.springbatchkafka.consumer;

import java.util.List;
import java.util.Properties;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;

import com.example.springbatchkafka.beans.Book;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@SpringBootApplication
@EnableBatchProcessing
@RequiredArgsConstructor
@Log4j2
public class SpringBatchKafkaConsumerApplication {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final KafkaProperties kafkaProperties;

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchKafkaConsumerApplication.class, args);
	}

	@Bean
	Job job() {
		return this.jobBuilderFactory.get("job").incrementer(new RunIdIncrementer()).start(step()).build();
	}

	@Bean
	KafkaItemReader<Long, Book> kafkaItemReader() {
		Properties props = new Properties();
		props.putAll(this.kafkaProperties.buildConsumerProperties());
		return new KafkaItemReaderBuilder<Long, Book>().partitions(0).consumerProperties(props).topic("Books")
				.name("books-reader").saveState(true).build();
	}

	@Bean
	Step step() {
		ItemWriter<Book> writer = new ItemWriter<Book>() {
			@Override
			public void write(List<? extends Book> items) throws Exception {
				items.forEach(it -> log.info("Book: " + it));
			}
		};
		return this.stepBuilderFactory.get("step").<Book, Book>chunk(10).writer(writer).reader(kafkaItemReader())
				.build();
	}
}
