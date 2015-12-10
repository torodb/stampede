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

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.apache.commons.lang.CharEncoding;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import com.github.fge.jsonschema.SchemaVersion;
import com.github.fge.jsonschema.cfg.ValidationConfiguration;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ListProcessingReport;
import com.github.fge.jsonschema.core.report.ListReportProvider;
import com.github.fge.jsonschema.core.report.LogLevel;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.main.JsonValidator;
import com.torodb.CliConfig;
import com.torodb.config.model.Config;

public class ConfigUtils {

	public static Config readConfig(final CliConfig cliConfig) throws FileNotFoundException, JsonProcessingException,
			IOException, JsonParseException, JsonMappingException, Exception {
		Config config = new Config();
		
		if (cliConfig.getConfFile() != null || cliConfig.getXmlConfFile() != null) {
			ObjectMapper mapper = null;
			InputStream validationInputStream = null;
			InputStream inputStream = null;
			if (cliConfig.getConfFile() != null) {
				mapper = new YAMLMapper();
				validationInputStream = cliConfig.getConfInputStream();
				inputStream = cliConfig.getConfInputStream();
			} else 
			if (cliConfig.getXmlConfFile() != null) {
				mapper = new XmlMapper();
				validationInputStream = cliConfig.getXmlConfInputStream();
				inputStream = cliConfig.getXmlConfInputStream();
			}
			
			if (inputStream != null) {
				validateWithJackson(mapper, validationInputStream);

				config = mapper.readValue(inputStream, Config.class);
				
				validateBean(config);
			}
		}
		
		if (cliConfig.getParams() != null) {
			YAMLMapper yamlMapper = new YAMLMapper();
			yamlMapper.setSerializationInclusion(Include.NON_NULL);
			JsonNode configNode = yamlMapper.valueToTree(config);
			for (String paramPathValue : cliConfig.getParams()) {
				int paramPathValueSeparatorIndex = paramPathValue.indexOf('=');
				String pathAndProp = paramPathValue.substring(0, paramPathValueSeparatorIndex);
				
				if (pathAndProp.startsWith("/")) {
					pathAndProp = pathAndProp.substring(1);
				}
				
				pathAndProp = "/" + pathAndProp;
				
				String value = paramPathValue.substring(paramPathValueSeparatorIndex + 1);
				
				mergeParam(yamlMapper, configNode, pathAndProp, value);
			}
			
			validateWithJackson(configNode);

			config = yamlMapper.treeToValue(configNode, Config.class);
			
			validateBean(config);
		}
		return config;
	}

	private static void mergeParam(ObjectMapper objectMapper, JsonNode configRootNode, String pathAndProp, String value)
					throws Exception {
		String path = pathAndProp.substring(0, pathAndProp.lastIndexOf("/"));
		String prop = pathAndProp.substring(pathAndProp.lastIndexOf("/") + 1);
		
		JsonPointer pathPointer = JsonPointer.compile(path); 
		JsonNode pathNode = configRootNode.at(pathPointer);
		
		if (pathNode.isMissingNode() || pathNode.isNull()) {
			JsonPointer currentPointer = pathPointer;
			JsonPointer childOfCurrentPointer = null;
			List<JsonPointer> missingPointers = new ArrayList<JsonPointer>();
			List<JsonPointer> childOfMissingPointers = new ArrayList<JsonPointer>();
			do {
				if(pathNode.isMissingNode() || pathNode.isNull()) {
					missingPointers.add(0, currentPointer);
					childOfMissingPointers.add(0, childOfCurrentPointer);
				}
				
				childOfCurrentPointer = currentPointer;
				currentPointer = currentPointer.head();
				pathNode = configRootNode.at(currentPointer);
			} while(pathNode.isMissingNode() || pathNode.isNull());
			
			for (int missingPointerIndex = 0; missingPointerIndex < missingPointers.size(); missingPointerIndex++) {
				final JsonPointer missingPointer = missingPointers.get(missingPointerIndex);
				final JsonPointer childOfMissingPointer = childOfMissingPointers.get(missingPointerIndex);
				
				final List<JsonNode> newNodes = new ArrayList<JsonNode>();
				
				if (pathNode.isObject()) {
					((ObjectNode) pathNode).set(missingPointer.last().getMatchingProperty(), createNode(childOfMissingPointer, newNodes));
				} else if (pathNode.isArray() && missingPointer.last().mayMatchElement()) {
					for (int index = ((ArrayNode) pathNode).size(); index < missingPointer.last().getMatchingIndex() + 1; index++) { 
						((ArrayNode) pathNode).add(createNode(childOfMissingPointer, newNodes));
					}
				} else {
					throw new RuntimeException("Cannot set param " + pathAndProp + "=" + value);
				}
				
				pathNode = newNodes.get(newNodes.size() - 1);
			}
		}
		
		ObjectNode objectNode = (ObjectNode) pathNode;
		Object valueAsObject = objectMapper.readValue(value, Object.class);
		if (valueAsObject != null) {
			JsonNode valueNode = objectMapper.valueToTree(valueAsObject);
			objectNode.set(prop, valueNode);
		} else {
			objectNode.remove(prop);
		}
	}
	
	private static JsonNode createNode(JsonPointer childOfPointer, List<JsonNode> newNodes) {
		JsonNode newNode = null;
		
		if (childOfPointer == null || !childOfPointer.last().mayMatchElement()) {
			newNode = JsonNodeFactory.instance.objectNode();
		} else {
			newNode = JsonNodeFactory.instance.arrayNode();
		}
		
		newNodes.add(newNode);
		
		return newNode;
	}

	public static ResourceBundle extractParamDescriptionFromConfigSchema(final ResourceBundle resourceBundle)
			throws UnsupportedEncodingException, JsonMappingException {
		final Properties properties = new Properties();
		ResourceBundle bundle = new ResourceBundle() {

			@Override
			protected Object handleGetObject(String key) {
				if (key == null) {
					throw new NullPointerException();
				}
				
				if (properties.containsKey(key)) {
					return properties.getProperty(key);
				} else {
					return resourceBundle.getObject(key);
				}
			}

			@Override
			public Enumeration<String> getKeys() {
				final Enumeration<Object> keys = properties.keys();
				final Enumeration<String> resourceBundleKeys = resourceBundle.getKeys();
				return new Enumeration<String>() {
					@Override
					public boolean hasMoreElements() {
						return keys.hasMoreElements() || resourceBundleKeys.hasMoreElements();
					}
					@Override
					public String nextElement() {
						Object nextElement = keys.nextElement();
						if (nextElement == null) {
							if (keys.hasMoreElements()) {
								return null;
							} else {
								return resourceBundleKeys.nextElement();
							}
						}
						if (nextElement instanceof String) {
							return (String) keys.nextElement();
						}
						return nextElement.toString();
					}
				};
			}
		};
		ByteArrayOutputStream paramByteArrayOutputStream = new ByteArrayOutputStream();
		PrintStream paramPrintStream = new PrintStream(paramByteArrayOutputStream, false, CharEncoding.UTF_8);
		
		paramPrintStream.println(resourceBundle.getString("param"));
		
		extractDescription(paramPrintStream);
		
		properties.setProperty("param", paramByteArrayOutputStream.toString(CharEncoding.UTF_8));
		
		return bundle;
	}

	private static void extractDescription(PrintStream paramPrintStream) throws JsonMappingException {
		ObjectMapper objectMapper = new ObjectMapper();
		ResourceBundle resourceBundle = PropertyResourceBundle.getBundle("ConfigMessages");
		DescriptionFactoryWrapper visitor = new DescriptionFactoryWrapper(resourceBundle, paramPrintStream);
		objectMapper.acceptJsonFormatVisitor(objectMapper.constructType(Config.class), visitor);
	}

	private static void validateWithJackson(ObjectMapper objectMapper, InputStream inputStream)
			throws JsonProcessingException, IOException {
		JsonNode configNode = objectMapper.readTree(inputStream);
		validateJsonSchema(objectMapper, configNode);
	}

	private static void validateWithJackson(JsonNode configNode) throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		validateJsonSchema(objectMapper, configNode);
	}

	private static void validateJsonSchema(ObjectMapper objectMapper, JsonNode configNode) throws JsonMappingException {
		SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();
		objectMapper.acceptJsonFormatVisitor(objectMapper.constructType(Config.class), visitor);
		JsonSchema jsonSchema = visitor.finalSchema();
		JsonNode schemaNode = objectMapper.valueToTree(jsonSchema);
		JsonSchemaFactory factory = JsonSchemaFactory.newBuilder()
				.setValidationConfiguration(
						ValidationConfiguration.newBuilder().setDefaultVersion(SchemaVersion.DRAFTV3).freeze())
				.setReportProvider(new ListReportProvider(LogLevel.DEBUG, LogLevel.NONE)).freeze();
		JsonValidator validator = factory.getValidator();
		ProcessingReport processingReport = null;
		try {
			processingReport = validator.validate(schemaNode, configNode, true);
		} catch (ProcessingException processingException) {
			ListProcessingReport listProcessingReport = new ListProcessingReport();
			listProcessingReport.log(LogLevel.DEBUG, processingException.getProcessingMessage());
			processingReport = listProcessingReport;
		}

		if (!processingReport.isSuccess()) {
			StringBuilder constraintViolationExceptionMessageBuilder = new StringBuilder();
			for (ProcessingMessage processingMessage : processingReport) {
				JsonNode messageNode = processingMessage.asJson();
				String message = messageNode.has("message") ? messageNode.get("message").textValue() : "unknown";
				String pointer = messageNode.has("instance") && messageNode.get("instance").isObject()
						&& ((ObjectNode) messageNode.get("instance")).has("pointer")
								? ((ObjectNode) messageNode.get("instance")).get("pointer").textValue() : "?";
				if (constraintViolationExceptionMessageBuilder.length() > 0) {
					constraintViolationExceptionMessageBuilder.append(", ");
				}
				constraintViolationExceptionMessageBuilder.append(pointer);
				constraintViolationExceptionMessageBuilder.append(": ");
				constraintViolationExceptionMessageBuilder.append(message);
			}
			throw new IllegalArgumentException(constraintViolationExceptionMessageBuilder.toString());
		}
	}

	public static void validateBean(Config config) {
		ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		Set<ConstraintViolation<Config>> constraintViolations = validator.validate(config);
		if (!constraintViolations.isEmpty()) {
			StringBuilder constraintViolationExceptionMessageBuilder = new StringBuilder();
			for (ConstraintViolation<Config> constraintViolation : constraintViolations) {
				if (constraintViolationExceptionMessageBuilder.length() > 0) {
					constraintViolationExceptionMessageBuilder.append(", ");
				}
				Path path = constraintViolation.getPropertyPath();
				JsonPointer pointer = toJsonPointer(path);
				constraintViolationExceptionMessageBuilder.append(pointer.toString());
				constraintViolationExceptionMessageBuilder.append(": ");
				constraintViolationExceptionMessageBuilder.append(constraintViolation.getMessage());
			}
			throw new IllegalArgumentException(constraintViolationExceptionMessageBuilder.toString());
		}
	}

	public static JsonPointer toJsonPointer(Path path) {
		JsonPointer pointer = JsonPointer.valueOf(null);
		for (Path.Node pathNode : path) {
			if (pathNode.getIndex() != null) {
				pointer = pointer.append(JsonPointer.valueOf("/" + pathNode.getIndex()));
			} else if (pathNode.getName() != null) {
				pointer = pointer.append(JsonPointer.valueOf("/" + pathNode.getName()));
			}
		}
		return pointer;
	}

}
