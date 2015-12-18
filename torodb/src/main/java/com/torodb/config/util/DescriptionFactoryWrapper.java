/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.config.util;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.ResourceBundle;

import com.beust.jcommander.internal.Console;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonAnyFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonArrayFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitable;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonMapFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.google.common.base.CaseFormat;
import com.torodb.config.annotation.Description;

public class DescriptionFactoryWrapper extends JsonFormatVisitorWrapper.Base {
	
	private final ResourceBundle resourceBundle;
	private final Console console;
	private final int tabs;
	private final JsonPointer jsonPointer;
	
	public DescriptionFactoryWrapper(ResourceBundle resourceBundle, Console console, int tabs) {
		this.resourceBundle = resourceBundle;
		this.console = console;
		this.tabs = tabs;
		this.jsonPointer = JsonPointer.valueOf(null);
	}
	
	public DescriptionFactoryWrapper(DescriptionFactoryWrapper descriptionFactoryWrapper, JsonPointer jsonPointer, SerializerProvider p) {
		this.resourceBundle = descriptionFactoryWrapper.resourceBundle;
		this.console = descriptionFactoryWrapper.console;
		this.tabs = descriptionFactoryWrapper.tabs;
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
		s.acceptJsonFormatVisitor(new DescriptionFactoryWrapper(this, getJsonPointer(), p), type);
		
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
				s.acceptJsonFormatVisitor(new DescriptionFactoryWrapper(DescriptionFactoryWrapper.this, jsonPointer.append(JsonPointer.valueOf("/<index>")), p), elementType);
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
				s.acceptJsonFormatVisitor(new DescriptionFactoryWrapper(DescriptionFactoryWrapper.this, propPointer, p), prop.getType());
			}
		};
	}

	private void document(JsonPointer propPointer, BeanProperty prop) {
		JavaType type = prop.getType();
		
		if (hasDescription(prop) && !isPrimitive(type) && !type.isEnumType() && !type.isMapLikeType()) {
			console.println("");
		} else
		if (isPrimitive(type) || type.isEnumType()) {
			printTabs();
			console.print(propPointer.toString());
			console.print("=");
		} else
		if (type.isMapLikeType()) {
			printTabs();
			console.print(propPointer.append(JsonPointer.compile("/<string>")).toString());
			console.print("=");
			type = type.getContentType();
		}
		
		if (isPrimitive(type)) {
			console.print("<");
			console.print(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, type.getRawClass().getSimpleName()));
			console.print(">");
		} else if (type.isEnumType()) {
			console.print("<enum:string>");
		}
		
		if (hasDescription(prop) && !isPrimitive(type) && !type.isEnumType()) {
			printTabs();
		}
		
		printDescription(prop);
		
		if (hasDescription(prop) || isPrimitive(type) || type.isEnumType()) {
			console.println("");
		}
		
		if (hasDescription(prop) && !isPrimitive(type) && !type.isEnumType()) {
			console.println("");
		}
		
		if (type.isEnumType()) {
			for (Field enumField : type.getRawClass().getDeclaredFields()) {
				if (!enumField.isEnumConstant()) {
					continue;
				}

				printTabs();
				console.print(" - ");
				console.print(enumField.getName());

				Description enumConstantConfigProperty = enumField.getAnnotation(Description.class);
				if (enumConstantConfigProperty != null && enumConstantConfigProperty.value() != null) {
					console.print(" # ");
					console.print(enumConstantConfigProperty.value());
				}

				console.println("");
			}
		}
	}

	private void printTabs() {
		for (int tab = 0; tab < tabs; tab++) {
			console.print("\t");
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
			console.print(" # ");
			console.print(resourceBundle.getString(description.value()));
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
