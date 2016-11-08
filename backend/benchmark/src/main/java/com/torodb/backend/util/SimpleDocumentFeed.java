/*
 * MongoWP - ToroDB-poc: Backends benchmark
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Charsets;
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
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(is,Charsets.UTF_8))) {
				StringBuffer sb = new StringBuffer();
				String line = null;
				while ((line = reader.readLine()) != null) {
					sb.append(line);
				}
				return sb.toString();
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
