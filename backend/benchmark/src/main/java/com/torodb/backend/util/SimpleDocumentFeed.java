package com.torodb.backend.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.torodb.kvdocument.conversion.json.GsonJsonParser;
import com.torodb.kvdocument.values.KVDocument;

public class SimpleDocumentFeed {

	public long datasize = 0;
	public long documents = 0;

	private int times = 0;

	public SimpleDocumentFeed(int times) {
		this.times = times;
	}
	
	public static KVDocument loadDocument(String name) {
		String document = getDocument(name);
		GsonJsonParser parser = new GsonJsonParser();
		return parser.createFromJson(document);
	}

	public Stream<KVDocument> getFeed(String name) {
		String document = getDocument(name);
		GsonJsonParser parser = new GsonJsonParser();
		return IntStream.range(0, times).boxed().map(i -> {
			if (i % 10000 == 0) {
				System.out.println(i + " documents");
			}
			datasize += document.length();
			documents++;
			return parser.createFromJson(document);
		});
	}

	private static String getDocument(String name) {
		try (InputStream is = SimpleDocumentFeed.class.getClassLoader().getResourceAsStream(name)) {
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
				StringBuffer sb = new StringBuffer();
				String line = null;
				while ((line = reader.readLine()) != null) {
					sb.append(line);
				}
				return sb.toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
