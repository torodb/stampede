package com.torodb.util.xsd;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.stax.StAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

import com.sun.xml.xsom.XSAnnotation;
import com.sun.xml.xsom.XSElementDecl;
import com.sun.xml.xsom.XSFacet;
import com.sun.xml.xsom.XSParticle;
import com.sun.xml.xsom.XSSchema;
import com.sun.xml.xsom.XSSchemaSet;
import com.sun.xml.xsom.XSTerm;
import com.sun.xml.xsom.XSType;
import com.sun.xml.xsom.parser.XSOMParser;

public class XsdUtils {

	public static void extractDescriptionFromXsd(String xsdResource, String namespace, PrintStream paramPrintStream)
			throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException, SAXException {
		SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
		XSOMParser parser = new XSOMParser(saxParserFactory);
		parser.setAnnotationParser(new XsdAnnotationFactory());
		parser.parse(Class.class.getResource(xsdResource));
		XSSchemaSet configSchemaSet = parser.getResult();

		Iterator<XSSchema> configSchemaIterator = configSchemaSet.iterateSchema();
		
		while (configSchemaIterator.hasNext()) {
			XSSchema configSchema = configSchemaIterator.next();

			if (!namespace.equals(configSchema.getTargetNamespace())) {
				continue;
			}
			
			List<PathElementDeclIterator> pathElementDeclsIteratorList = new ArrayList<PathElementDeclIterator>();
			List<Element> configSchemaElements = new ArrayList<Element>();
			Iterator<XSElementDecl> configSchemaElementDeclsIterator = configSchema.iterateElementDecls();
			while (configSchemaElementDeclsIterator.hasNext()) {
				configSchemaElements.add(new Element(configSchemaElementDeclsIterator.next(), ElementType.ROOT));
			}
			pathElementDeclsIteratorList.add(new PathElementDeclIterator(configSchemaElements.iterator()));
			
			do {
				PathElementDeclIterator pathElementIterator = pathElementDeclsIteratorList.get(0);
				Element element = pathElementIterator.getElementIterator().next();
				ElementType type = element.getType();
				XSElementDecl e = element.getElementDecl();
			    
				if (!e.getType().isComplexType()) {
					for (String path : pathElementIterator.getPath()) {
						paramPrintStream.print( path );
						paramPrintStream.print( "." );
					}
					paramPrintStream.print( e.getName() );
					paramPrintStream.print("=");
				}
				
				if (e.getType().isComplexType()) {
					XSAnnotation a = e.getType().asComplexType().getAnnotation();
					if (a != null && a.getAnnotation() != null) {
						paramPrintStream.println();
						paramPrintStream.print(" # ");
						paramPrintStream.println( a.getAnnotation().toString() );
						paramPrintStream.println();
					}
					
					XSParticle contentParticle = e.getType().asComplexType().getContentType().asParticle();
					
					int index = 0;
					List<XSParticle> particleList = new ArrayList<XSParticle>();
					particleList.add(contentParticle);


					do {
						XSParticle particle = particleList.remove(0);
						XSTerm term = particle.getTerm();
						if (term.isModelGroup()) {
							int particleIndex = 0;
							for (XSParticle childParticle : term.asModelGroup().getChildren()) {
								particleList.add(particleIndex++, childParticle);
							}
						} else if (term.isElementDecl()) {
							List<String> paths = new ArrayList<String>();
							
							if (type == ElementType.SCALAR) {
								paths.add(e.getName());
							} else if (type == ElementType.ARRAY) {
								paths.add(e.getName());
								paths.add("<index>");
							}
							
							ElementType childType = ElementType.SCALAR;
							
							if (particle.isRepeated()) {
								childType = ElementType.ARRAY;
							}
							
							pathElementDeclsIteratorList.add(index++, 
								new PathElementDeclIterator(
									pathElementIterator
								,	new Element(term.asElementDecl(), childType)
								, 	paths.toArray(new String[paths.size()])
								));
						}
					} while(!particleList.isEmpty());
				} else if (e.getType().isSimpleType()) {
					if (e.getType().asSimpleType().isPrimitive()) {
						paramPrintStream.print("<");
						paramPrintStream.print(e.getType().asSimpleType().getName());
						paramPrintStream.print(">");

						XSAnnotation a = e.getAnnotation();
						if (a != null && a.getAnnotation() != null) {
							paramPrintStream.print(" # ");
							paramPrintStream.print( a.getAnnotation().toString() );
						}
						paramPrintStream.println();
					} else if (e.getType().asSimpleType().isRestriction()) {
						boolean isEnumeration = false;
						for (XSFacet facet : e.getType().asSimpleType().asRestriction().getDeclaredFacets()) {
							if ("enumeration".equals(facet.getName())) {
								isEnumeration = true;
								break;
							}
						}
						
						if (isEnumeration) {
							paramPrintStream.print("<enum:");
							paramPrintStream.print(e.getType().asSimpleType().asRestriction().getSimpleBaseType().getName());
							paramPrintStream.print(">");

							XSAnnotation a = e.getAnnotation();
							if (a != null && a.getAnnotation() != null) {
								paramPrintStream.print(": ");
								paramPrintStream.print( a.getAnnotation().toString() );
							}

							paramPrintStream.print(" [");
							boolean firstEnumeration = true;
							for (XSFacet facet : e.getType().asSimpleType().asRestriction().getDeclaredFacets()) {
								if ("enumeration".equals(facet.getName())) {
									if (!firstEnumeration) {
										paramPrintStream.print(" | ");
									}
									firstEnumeration = false;
									paramPrintStream.print(facet.getValue().toString());

									XSAnnotation aFacet = facet.getAnnotation();
									if (aFacet != null && aFacet.getAnnotation() != null) {
										paramPrintStream.print(" # ");
										paramPrintStream.print( aFacet.getAnnotation().toString() );
									}
								}
							}
							paramPrintStream.println("]");
						} else {
							paramPrintStream.print("<");
							paramPrintStream.print(e.getType().asSimpleType().getName());
							paramPrintStream.print(">");

							XSAnnotation a = e.getAnnotation();
							if (a != null && a.getAnnotation() != null) {
								paramPrintStream.print(" # ");
								paramPrintStream.print( a.getAnnotation().toString() );
							}
							paramPrintStream.println();
						}
					}
				}
				
				while (!pathElementDeclsIteratorList.isEmpty() && !pathElementDeclsIteratorList.get(0).getElementIterator().hasNext()) {
					pathElementDeclsIteratorList.remove(0);
				}
			} while(!pathElementDeclsIteratorList.isEmpty());
		}
	}

	public static void validateWithXsd(String resourceXsd, InputStream inputStream) throws SAXException, IOException, XMLStreamException, FactoryConfigurationError {
		SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Schema schema = factory.newSchema(Class.class.getResource(resourceXsd));
		Validator validator = schema.newValidator();
		validator.validate(new StAXSource(XMLInputFactory.newInstance().createXMLStreamReader(inputStream)));
	}
	
	private enum ElementType {
		ROOT,
		SCALAR,
		ARRAY,
		CHOICE
	}

	private static class Element {
		private final XSElementDecl elementDecl;
		private final ElementType type;
		public Element(XSElementDecl elementDecl, ElementType type) {
			super();
			this.elementDecl = elementDecl;
			this.type = type;
		}
		public XSElementDecl getElementDecl() {
			return elementDecl;
		}
		public ElementType getType() {
			return type;
		}
	}
	
	private static class PathElementDeclIterator {
		private final List<String> path;
		private final Iterator<Element> elementIterator;

		public PathElementDeclIterator(Iterator<Element> elementIterator) {
			super();
			this.path = new ArrayList<String>();
			this.elementIterator = elementIterator;
		}
		public PathElementDeclIterator(PathElementDeclIterator pathElementDeclIterator, Element element, String...nextPaths) {
			super();
			this.path = new ArrayList<String>(pathElementDeclIterator.path);
			List<Element> elements = new ArrayList<Element>();
			elements.add(element);
			this.elementIterator = elements.iterator();
			
			if (nextPaths != null) {
				for (String nextPath : nextPaths) {
					path.add(nextPath);
				}
			}
		}
		public Iterator<Element> getElementIterator() {
			return elementIterator;
		}
		public List<String> getPath() {
			return path;
		}
	}

}
