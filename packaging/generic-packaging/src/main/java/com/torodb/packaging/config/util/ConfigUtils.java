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

package com.torodb.packaging.config.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.beust.jcommander.internal.Console;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonMappingException.Reference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.Charsets;
import com.google.common.net.HostAndPort;
import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.packaging.config.model.backend.postgres.Postgres;
import com.torodb.packaging.config.model.protocol.mongo.Mongo;
import com.torodb.packaging.config.model.protocol.mongo.Replication;

public class ConfigUtils {

    private static final Logger LOGGER = LogManager.getLogger(ConfigUtils.class);

    public static ObjectMapper mapper() {
        ObjectMapper objectMapper = new ObjectMapper();

        configMapper(objectMapper);

        return objectMapper;
    }

    public static YAMLMapper yamlMapper() {
        YAMLMapper yamlMapper = new YAMLMapper();

        configMapper(yamlMapper);

        return yamlMapper;
    }

    public static XmlMapper xmlMapper() {
        XmlMapper xmlMapper = new XmlMapper();

        configMapper(xmlMapper);

        return xmlMapper;
    }

    private static void configMapper(ObjectMapper objectMapper) {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, true);
        objectMapper.configure(Feature.ALLOW_COMMENTS, true);
        objectMapper.configure(Feature.ALLOW_YAML_COMMENTS, true);
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
    
    public static IllegalArgumentException transformJsonMappingException(JsonMappingException jsonMappingException) {
        JsonPointer jsonPointer = JsonPointer.compile("/config");
        for (Reference reference : jsonMappingException.getPath()) {
            if (reference.getIndex() != -1) {
                jsonPointer = jsonPointer.append(JsonPointer.compile("/" + reference.getIndex()));
            }
            if (reference.getFieldName() != null) {
                jsonPointer = jsonPointer.append(JsonPointer.compile("/" + reference.getFieldName()));
            }
        }
        
        if (LOGGER.isDebugEnabled()) {
            return new IllegalArgumentException("Validation error at " + jsonPointer + ": " + jsonMappingException.getMessage(), jsonMappingException);
        }
        
        return new IllegalArgumentException("Validation error at " + jsonPointer + ": " + jsonMappingException.getMessage());
    }

    public static Config readConfigFromYaml(String yamlString) throws JsonProcessingException, IOException {
        ObjectMapper objectMapper = mapper();
        YAMLMapper yamlMapper = yamlMapper();
        
        JsonNode configNode = yamlMapper.readTree(yamlString);

        Config config = objectMapper.treeToValue(configNode, Config.class);

        validateBean(config);

        return config;
    }

    public static Config readConfigFromXml(String xmlString) throws JsonProcessingException, IOException {
        ObjectMapper objectMapper = mapper();
        XmlMapper xmlMapper = xmlMapper();
        
        JsonNode configNode = xmlMapper.readTree(xmlString);

        Config config = objectMapper.treeToValue(configNode, Config.class);

        validateBean(config);

        return config;
    }

    public static void parseToropassFile(final Config config) throws FileNotFoundException, IOException {
        if (config.getBackend().isPostgresLike()) {
            Postgres postgres = config.getBackend().asPostgres();
            postgres.setPassword(getPasswordFromPassFile(
                    postgres.getToropassFile(), 
                    postgres.getHost(), 
                    postgres.getPort(), 
                    postgres.getDatabase(), 
                    postgres.getUser()));
        } else if(config.getBackend().isDerbyLike()) {
            Derby derby = config.getBackend().asDerby();
            derby.setPassword(getPasswordFromPassFile(
                    derby.getToropassFile(), 
                    derby.getHost(), 
                    derby.getPort(), 
                    derby.getDatabase(), 
                    derby.getUser()));
        }
    }

    public static void parseMongopassFile(final Config config) throws FileNotFoundException, IOException {
        Mongo mongo = config.getProtocol().getMongo();
        if (mongo.getReplication() != null) {
            for (Replication replication : mongo.getReplication()) {
                if (replication.getAuth().getUser() != null) {
                    HostAndPort syncSource = HostAndPort.fromString(replication.getSyncSource())
                            .withDefaultPort(27017);
                    replication.getAuth().setPassword(getPasswordFromPassFile(
                            mongo.getMongopassFile(), 
                            syncSource.getHostText(), 
                            syncSource.getPort(), 
                            replication.getAuth().getSource(), 
                            replication.getAuth().getUser()));
                }
            }
        }
    }

    private static String getPasswordFromPassFile(String passFile, String host, int port, String database,
            String user) throws FileNotFoundException, IOException {
        File pass = new File(passFile);
        if (pass.exists() && pass.canRead() && pass.isFile()) {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(pass), Charsets.UTF_8));
            try {
                String line;
                int index = 0;
                while ((line = br.readLine()) != null) {
                    index++;
                    String[] passChunks = line.split(":");
                    if (passChunks.length != 5) {
                        LOGGER.warn("Wrong format at line " + index + " of file " + passFile);
                        continue;
                    }

                    if ((passChunks[0].equals("*") || passChunks[0].equals(host))
                            && (passChunks[1].equals("*")
                                    || passChunks[1].equals(String.valueOf(port)))
                            && (passChunks[2].equals("*") || passChunks[2].equals(database))
                            && (passChunks[3].equals("*") || passChunks[3].equals(user))) {
                        return passChunks[4];
                    }
                }
                br.close();
            } finally {
                br.close();
            }
        }
        
        return null;
    }

	public static ObjectNode mergeParam(ObjectMapper objectMapper, ObjectNode configRootNode, String pathAndProp, String value)
			throws Exception {
	    if (JsonPointer.compile(pathAndProp).equals(JsonPointer.compile("/"))) {
	        return (ObjectNode) objectMapper.readTree(value);
	    }
	    
		String path = pathAndProp.substring(0, pathAndProp.lastIndexOf("/"));
		String prop = pathAndProp.substring(pathAndProp.lastIndexOf("/") + 1);

		JsonPointer pathPointer = JsonPointer.compile(path);
		JsonNode pathNode = configRootNode.at(pathPointer);

		if (pathNode.isMissingNode() || pathNode.isNull()) {
			JsonPointer currentPointer = pathPointer;
			JsonPointer childOfCurrentPointer = null;
			List<JsonPointer> missingPointers = new ArrayList<>();
			List<JsonPointer> childOfMissingPointers = new ArrayList<>();
			do {
				if (pathNode.isMissingNode() || pathNode.isNull()) {
					missingPointers.add(0, currentPointer);
					childOfMissingPointers.add(0, childOfCurrentPointer);
				}

				childOfCurrentPointer = currentPointer;
				currentPointer = currentPointer.head();
				pathNode = configRootNode.at(currentPointer);
			} while (pathNode.isMissingNode() || pathNode.isNull());

			for (int missingPointerIndex = 0; missingPointerIndex < missingPointers.size(); missingPointerIndex++) {
				final JsonPointer missingPointer = missingPointers.get(missingPointerIndex);
				final JsonPointer childOfMissingPointer = childOfMissingPointers.get(missingPointerIndex);

				final List<JsonNode> newNodes = new ArrayList<>();

				if (pathNode.isObject()) {
					((ObjectNode) pathNode).set(missingPointer.last().getMatchingProperty(),
							createNode(childOfMissingPointer, newNodes));
				} else if (pathNode.isArray() && missingPointer.last().mayMatchElement()) {
					for (int index = ((ArrayNode) pathNode).size(); index < missingPointer.last().getMatchingIndex()
							+ 1; index++) {
						((ArrayNode) pathNode).add(createNode(childOfMissingPointer, newNodes));
					}
				} else {
					throw new RuntimeException("Cannot set param " + pathAndProp + "=" + value);
				}

				pathNode = newNodes.get(newNodes.size() - 1);
			}
		}

		Object valueAsObject;
		try {
		    valueAsObject = objectMapper.readValue(value, Object.class);
		} catch(JsonMappingException jsonMappingException) {
		    throw JsonMappingException.wrapWithPath(jsonMappingException, configRootNode, path.substring(1) + "/" + prop);
		}
		
		if (pathNode instanceof ObjectNode) {
	        ObjectNode objectNode = (ObjectNode) pathNode;
	        if (valueAsObject != null) {
	            JsonNode valueNode = objectMapper.valueToTree(valueAsObject);
	            objectNode.set(prop, valueNode);
	        } else {
	            objectNode.remove(prop);
	        }
		} else if (pathNode instanceof ArrayNode) {
	        ArrayNode arrayNode = (ArrayNode) pathNode;
	        Integer index = Integer.valueOf(prop);
	        if (valueAsObject != null) {
	            JsonNode valueNode = objectMapper.valueToTree(valueAsObject);
	            arrayNode.set(index, valueNode);
	        } else {
	            arrayNode.remove(index);
	        }
		}
		
		return configRootNode;
	}

	private static JsonNode createNode(JsonPointer childOfPointer, List<JsonNode> newNodes) {
		JsonNode newNode;

		if (childOfPointer == null || !childOfPointer.last().mayMatchElement()) {
			newNode = JsonNodeFactory.instance.objectNode();
		} else {
			newNode = JsonNodeFactory.instance.arrayNode();
		}

		newNodes.add(newNode);

		return newNode;
	}

	public static void printYamlConfig(Config config, Console console)
			throws IOException, JsonGenerationException, JsonMappingException {
		ObjectMapper objectMapper = yamlMapper();
		ObjectWriter objectWriter = objectMapper.writer();
		printConfig(config, console, objectWriter);
	}

	public static void printXmlConfig(Config config, Console console)
			throws IOException, JsonGenerationException, JsonMappingException {
		ObjectMapper objectMapper = xmlMapper();
		ObjectWriter objectWriter = objectMapper.writer();
		objectWriter = objectWriter.withRootName("config");
		printConfig(config, console, objectWriter);
	}

	private static void printConfig(Config config, Console console, ObjectWriter objectWriter)
			throws IOException, JsonGenerationException, JsonMappingException, UnsupportedEncodingException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		PrintStream printStream = new PrintStream(byteArrayOutputStream, false, Charsets.UTF_8.name());
		objectWriter.writeValue(printStream, config);
		console.println(byteArrayOutputStream.toString(Charsets.UTF_8.name()));
	}

	public static void printParamDescriptionFromConfigSchema(Console console, int tabs)
			throws UnsupportedEncodingException, JsonMappingException {
		ObjectMapper objectMapper = mapper();
		ResourceBundle resourceBundle = PropertyResourceBundle.getBundle("ConfigMessages");
		DescriptionFactoryWrapper visitor = new DescriptionFactoryWrapper(resourceBundle, console, tabs);
		objectMapper.acceptJsonFormatVisitor(objectMapper.constructType(Config.class), visitor);
		console.println("");
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
			}
			if (pathNode.getName() != null) {
				pointer = pointer.append(JsonPointer.valueOf("/" + pathNode.getName()));
			}
		}
		return pointer;
	}
}
