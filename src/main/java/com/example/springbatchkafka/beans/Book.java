package com.example.springbatchkafka.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Book {

	private Long id;
	private String name;
	// private String Auther;
}
