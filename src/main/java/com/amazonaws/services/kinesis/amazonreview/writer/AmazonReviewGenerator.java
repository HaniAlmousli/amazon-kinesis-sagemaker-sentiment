
package com.amazonaws.services.kinesis.amazonreview.writer;


import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileReader;
import java.io.BufferedReader;

import com.amazonaws.services.kinesis.amazonreviews.model.AmazonReview;;

public class AmazonReviewGenerator {

	private ObjectMapper objectMapper = new ObjectMapper();
	

	BufferedReader reader;

	public AmazonReviewGenerator(String file_location) {
		try {
			reader = new BufferedReader(new FileReader(file_location));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public AmazonReviewGenerator() {

		try {
			reader = new BufferedReader(new FileReader(
					AmazonReviewGenerator.class.getProtectionDomain().getCodeSource().getLocation().getPath()
							+ "/../../src/main/java/com/amazonaws/services/kinesis/amazonreview/writer/reviews.json"));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public AmazonReview getRandomReview() throws Exception {
		String line = reader.readLine();
		if (line == null) {
			reader.reset();
			line = reader.readLine();

		}
		AmazonReview review = this.objectMapper.readValue(line, AmazonReview.class);
		return review;

	}

}
