package com.torodb.util.xsd;

import com.sun.xml.xsom.parser.AnnotationParser;
import com.sun.xml.xsom.parser.AnnotationParserFactory;

public class XsdAnnotationFactory implements AnnotationParserFactory {
	@Override
	public AnnotationParser create() {
		return new XsdAnnotationParser();
	}
}