package com.example.springbatchkafka.producer;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import com.example.springbatchkafka.beans.Book;

import lombok.RequiredArgsConstructor;

@SpringBootApplication
@EnableBatchProcessing
@RequiredArgsConstructor
public class SpringbatchKafkaProducerApplication {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final KafkaTemplate<Long, Book> kafkaTemplate;

	public static void main(String[] args) {
		SpringApplication.run(SpringbatchKafkaProducerApplication.class, args);
	}

	@Bean
	Job job() {
		return this.jobBuilderFactory.get("job").start(step()).incrementer(new RunIdIncrementer()).build();
	}

	@Bean
	KafkaItemWriter<Long, Book> writer() {
		return new KafkaItemWriterBuilder<Long, Book>().kafkaTemplate(kafkaTemplate).itemKeyMapper(Book::getId).build();
	}

	@Bean
	Step step() {
		AtomicLong id = new AtomicLong();
		ItemReader<Book> reader = new ItemReader<Book>() {
			@Override
			public Book read() {
				if (id.incrementAndGet() < 10_000) {
					return new Book(id.get(), Math.random() > .5 ? "Eat Love and Pray" : "This is all I have to say");
				}
				return null;
			}

		};
		return this.stepBuilderFactory.get("step").<Book, Book>chunk(10).reader(reader).writer(writer()).build();
	}

}
