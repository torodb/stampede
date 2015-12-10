package com.torodb.config.util;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.ResourceBundle;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonAnyFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonArrayFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitable;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonMapFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.google.common.base.CaseFormat;
import com.torodb.config.annotation.Description;

public class DescriptionFactoryWrapper extends JsonFormatVisitorWrapper.Base {
	
	private final ResourceBundle resourceBundle;
	private final PrintStream printStream;
	private final JsonPointer jsonPointer;
	
	public DescriptionFactoryWrapper(ResourceBundle resourceBundle, PrintStream printStream) {
		this.resourceBundle = resourceBundle;
		this.printStream = printStream;
		this.jsonPointer = JsonPointer.valueOf(null);
	}
	
	public DescriptionFactoryWrapper(ResourceBundle resourceBundle, PrintStream printStream, JsonPointer jsonPointer, SerializerProvider p) {
		this.resourceBundle = resourceBundle;
		this.printStream = printStream;
		this.jsonPointer = jsonPointer;
		setProvider(p);
	}

	public JsonPointer getJsonPointer() {
		return jsonPointer;
	}

	@Override
	public JsonAnyFormatVisitor expectAnyFormat(JavaType type) throws JsonMappingException {
		SerializerProvider p = getProvider();
		JsonSerializer<Object> s = p.findValueSerializer(type);
		s.acceptJsonFormatVisitor(new DescriptionFactoryWrapper(resourceBundle, printStream, getJsonPointer(), p), type);
		
		return super.expectAnyFormat(type);
	}

	@Override
	public JsonArrayFormatVisitor expectArrayFormat(JavaType convertedType) {
		final JsonPointer jsonPointer = getJsonPointer();
		
		return new JsonArrayFormatVisitor.Base(getProvider()) {
			@Override
			public void itemsFormat(JsonFormatVisitable handler, JavaType elementType) throws JsonMappingException {
				SerializerProvider p = getProvider();
				JsonSerializer<Object> s = p.findValueSerializer(elementType);
				s.acceptJsonFormatVisitor(new DescriptionFactoryWrapper(resourceBundle, printStream, jsonPointer.append(JsonPointer.valueOf("/<index>")), p), elementType);
			}
		};
	}

	@Override
	public JsonObjectFormatVisitor expectObjectFormat(JavaType convertedType) {
		final JsonPointer jsonPointer = getJsonPointer();
		
		return new JsonObjectFormatVisitor.Base(getProvider()) {

			@Override
			public void property(BeanProperty prop) throws JsonMappingException {
				visitProperty(prop, false);
			}

			@Override
			public void optionalProperty(BeanProperty prop) throws JsonMappingException {
				visitProperty(prop, true);
			}

			private void visitProperty(BeanProperty prop, boolean optional) throws JsonMappingException {
				JsonPointer propPointer = jsonPointer.append(JsonPointer.valueOf("/" + prop.getName()));
				document(propPointer, prop);
				SerializerProvider p = getProvider();
				JsonSerializer<Object> s = p.findValueSerializer(prop.getType(), prop);
				s.acceptJsonFormatVisitor(new DescriptionFactoryWrapper(resourceBundle, printStream, propPointer, p), prop.getType());
			}
		};
	}

	private void document(JsonPointer propPointer, BeanProperty prop) {
		JavaType type = prop.getType();
		
		if (hasDescription(prop) && !isPrimitive(type) && !type.isEnumType() && !type.isMapLikeType()) {
			printStream.println();
		} else
		if (isPrimitive(type) || type.isEnumType()) {
			printStream.print(propPointer.toString());
			printStream.print("=");
		} else
		if (type.isMapLikeType()) {
			printStream.print(propPointer.append(JsonPointer.compile("/<string>")).toString());
			printStream.print("=");
		}
		
		if (type.isMapLikeType()) {
			type = type.getContentType();
		}
		
		if (isPrimitive(type)) {
			printStream.print("<");
			printStream.print(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, type.getRawClass().getSimpleName()));
			printStream.print(">");
		} else if (type.isEnumType() || (type.isMapLikeType() && type.getContentType().isEnumType())) {
			printStream.print("<enum:string>");
		}
		
		printDescription(prop);
		
		if (hasDescription(prop) || isPrimitive(type) || type.isEnumType() || type.isMapLikeType()) {
			printStream.println();
		}
		
		if (hasDescription(prop) && !isPrimitive(type) && !type.isEnumType() && !type.isMapLikeType()) {
			printStream.println();
		}
		
		if (type.isEnumType()) {
			for (Field enumField : type.getRawClass().getDeclaredFields()) {
				if (!enumField.isEnumConstant()) {
					continue;
				}

				printStream.print(" - ");
				printStream.print(enumField.getName());

				Description enumConstantConfigProperty = enumField.getAnnotation(Description.class);
				if (enumConstantConfigProperty != null && enumConstantConfigProperty.value() != null) {
					printStream.print(" # ");
					printStream.print(enumConstantConfigProperty.value());
				}

				printStream.println();
			}
		}
	}
	
	private boolean isPrimitive(JavaType type) {
		return type.isPrimitive()
				 || type.isTypeOrSubTypeOf(Boolean.class)
				 || type.isTypeOrSubTypeOf(Number.class)
				 || type.isTypeOrSubTypeOf(String.class)
				 || type.isTypeOrSubTypeOf(Character.class)
				 || type.isTypeOrSubTypeOf(Date.class);
	}
	
	private boolean hasDescription(BeanProperty prop) {
		return getDescription(prop) != null;
	}
	
	private void printDescription(BeanProperty prop) {
		Description description = getDescription(prop);
		if (description != null) {
			printStream.print(" # ");
			printStream.print(resourceBundle.getString(description.value()));
		}
	}
	
	private Description getDescription(BeanProperty prop) {
		Description description = prop.getAnnotation(Description.class);
		if (description == null) {
			description = prop.getType().getRawClass().getAnnotation(Description.class);
		}
		return description;
	}
	
	@Override
	public JsonMapFormatVisitor expectMapFormat(JavaType type) throws JsonMappingException {
		return new JsonMapFormatVisitor.Base(getProvider());
	}
}
