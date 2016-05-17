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

package com.torodb.config.jackson;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.PropertyMetadata;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.torodb.config.model.backend.Backend;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.mysql.MySQL;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.visitor.BackendImplementationVisitor;
import com.torodb.torod.core.exceptions.ToroRuntimeException;

public class BackendSerializer extends JsonSerializer<Backend> {
	@Override
	public void serialize(Backend value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonProcessingException {
		jgen.writeStartObject();

		value.getBackendImplementation().accept(new BackendImplementationSerializerVisitor(jgen));

		jgen.writeEndObject();
	}

	@Override
	public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType type) throws JsonMappingException {
		if (visitor == null) {
			return;
		}

		JsonObjectFormatVisitor v = visitor.expectObjectFormat(type);
		SerializerProvider prov = visitor.getProvider();
		final SerializationConfig config = prov.getConfig();
		BeanDescription beanDesc = config.introspect(type);
		JsonSubTypes jsonSubTypes;
		if (v != null) {
			for (BeanPropertyDefinition propDef : beanDesc.findProperties()) {
				if (propDef.isExplicitlyIncluded()) {
					jsonSubTypes = propDef.getPrimaryMember().getAnnotation(JsonSubTypes.class);
					
					if (jsonSubTypes != null) {
						for (JsonSubTypes.Type jsonSubType : jsonSubTypes.value()) {
							JavaType subType = TypeFactory.defaultInstance().constructType(jsonSubType.value());
							depositSchemaProperty(v, jsonSubType.name(), subType);
						}
					} else {
						depositSchemaProperty(v, propDef.getName(), propDef.getPrimaryMember().getType(beanDesc.bindingsForBeanType()));
					}
				}
			}
		}
	}

	private void depositSchemaProperty(JsonObjectFormatVisitor v, String name, JavaType type)
			throws JsonMappingException {
		BeanProperty prop = new BeanProperty.Std(PropertyName.construct(name), type, null,
				null, null, PropertyMetadata.STD_OPTIONAL) {
			@Override
			public void depositSchemaProperty(JsonObjectFormatVisitor v) {
				try {
					if (v != null) {
						if (isRequired()) {
							v.property(this);
						} else {
							v.optionalProperty(this);
						}
					}
				} catch (JsonMappingException jsonMappingException) {
					throw new RuntimeException(jsonMappingException);
				} 
			}
		};
		prop.depositSchemaProperty(v);
	}
	
	private static class BackendImplementationSerializerVisitor implements BackendImplementationVisitor {
	    
	    private final JsonGenerator jgen;
	    
	    private BackendImplementationSerializerVisitor(JsonGenerator jgen) {
	        this.jgen = jgen;
	    }
	    
        @Override
        public void visit(Postgres value) {
            try {
                jgen.writeObjectField("postgres", value);
            } catch(Exception exception) {
                throw new ToroRuntimeException(exception);
            }
        }

        @Override
        public void visit(Greenplum value) {
            try {
                jgen.writeObjectField("greenplum", value);
            } catch(Exception exception) {
                throw new ToroRuntimeException(exception);
            }
        }

        @Override
        public void visit(MySQL value) {
            try {
                jgen.writeObjectField("mysql", value);
            } catch(Exception exception) {
                throw new ToroRuntimeException(exception);
            }
        }
	}
}