/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.packaging.config.util;

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
import com.fasterxml.jackson.databind.exc.PropertyBindingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.Charsets;
import com.torodb.packaging.config.model.backend.BackendPasswordConfig;
import com.torodb.packaging.config.model.protocol.mongo.MongoPasswordConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import java.util.ResourceBundle;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

public class ConfigUtils {

  private static final Logger LOGGER = LogManager.getLogger(ConfigUtils.class);

  public static final String getUserHomePath() {
    return System.getProperty("user.home", ".");
  }

  public static final String getUserHomeFilePath(String file) {
    return getUserHomePath() + File.separatorChar + file;
  }

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
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
  }

  public static IllegalArgumentException transformJsonMappingException(
      JsonMappingException jsonMappingException) {
    JsonPointer jsonPointer = JsonPointer.compile("/config");
    for (Reference reference : jsonMappingException.getPath()) {
      if (reference.getIndex() != -1) {
        jsonPointer = jsonPointer.append(JsonPointer.compile("/" + reference.getIndex()));
      }
      if (reference.getFieldName() != null) {
        jsonPointer = jsonPointer.append(JsonPointer.compile("/" + reference.getFieldName()));
      }
    }

    if (jsonMappingException instanceof PropertyBindingException) {
      return transformJsonMappingException(jsonPointer,
          (PropertyBindingException) jsonMappingException);
    }

    return transformJsonMappingException(jsonPointer, jsonMappingException);
  }

  private static IllegalArgumentException transformJsonMappingException(JsonPointer jsonPointer,
      JsonMappingException jsonMappingException) {
    return transformJsonMappingException(jsonPointer, "Wrong value", jsonMappingException);
  }

  private static IllegalArgumentException transformJsonMappingException(JsonPointer jsonPointer,
      PropertyBindingException jsonMappingException) {
    return transformJsonMappingException(jsonPointer, "Unrecognized field " + jsonMappingException
        .getPropertyName() + " (known fields: " + jsonMappingException.getKnownPropertyIds() + ")",
        jsonMappingException);
  }

  private static IllegalArgumentException transformJsonMappingException(JsonPointer jsonPointer,
      String message, JsonMappingException jsonMappingException) {
    if (LOGGER.isDebugEnabled()) {
      return new IllegalArgumentException("Validation error at " + jsonPointer + ": " + message,
          jsonMappingException);
    }

    return new IllegalArgumentException("Validation error at " + jsonPointer + ": " + message);
  }

  public static <T> T readConfigFromYaml(Class<T> configClass, String yamlString) throws
      JsonProcessingException, IOException {
    ObjectMapper objectMapper = mapper();
    YAMLMapper yamlMapper = yamlMapper();

    JsonNode configNode = yamlMapper.readTree(yamlString);

    T config = objectMapper.treeToValue(configNode, configClass);

    validateBean(config);

    return config;
  }

  public static <T> T readConfigFromXml(Class<T> configClass, String xmlString) throws
      JsonProcessingException, IOException {
    ObjectMapper objectMapper = mapper();
    XmlMapper xmlMapper = xmlMapper();

    JsonNode configNode = xmlMapper.readTree(xmlString);

    T config = objectMapper.treeToValue(configNode, configClass);

    validateBean(config);

    return config;
  }

  public static void parseToropassFile(final BackendPasswordConfig backendPasswordConfig) throws
      FileNotFoundException, IOException {
    backendPasswordConfig.setPassword(getPasswordFromPassFile(
        backendPasswordConfig.getToropassFile(),
        backendPasswordConfig.getHost(),
        backendPasswordConfig.getPort(),
        backendPasswordConfig.getDatabase(),
        backendPasswordConfig.getUser()));
  }

  public static void parseMongopassFile(final MongoPasswordConfig mongoPasswordConfig) throws
      FileNotFoundException, IOException {
    mongoPasswordConfig.setPassword(getPasswordFromPassFile(
        mongoPasswordConfig.getMongopassFile(),
        mongoPasswordConfig.getHost(),
        mongoPasswordConfig.getPort(),
        mongoPasswordConfig.getDatabase(),
        mongoPasswordConfig.getUser()));
  }

  private static String getPasswordFromPassFile(String passFile, String host, int port,
      String database,
      String user) throws FileNotFoundException, IOException {
    File pass = new File(passFile);
    if (pass.exists() && pass.canRead() && pass.isFile()) {
      BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(pass),
          Charsets.UTF_8));
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

  public static ObjectNode mergeParam(ObjectMapper objectMapper, ObjectNode configRootNode,
      String pathAndProp, String value)
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
      }
      while (pathNode.isMissingNode() || pathNode.isNull());

      for (int missingPointerIndex = 0; missingPointerIndex < missingPointers.size();
          missingPointerIndex++) {
        final JsonPointer missingPointer = missingPointers.get(missingPointerIndex);
        final JsonPointer childOfMissingPointer = childOfMissingPointers.get(missingPointerIndex);

        final List<JsonNode> newNodes = new ArrayList<>();

        if (pathNode.isObject()) {
          ((ObjectNode) pathNode).set(missingPointer.last().getMatchingProperty(),
              createNode(childOfMissingPointer, newNodes));
        } else if (pathNode.isArray() && missingPointer.last().mayMatchElement()) {
          for (int index = ((ArrayNode) pathNode).size(); index < missingPointer.last()
              .getMatchingIndex()
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
    } catch (JsonMappingException jsonMappingException) {
      throw JsonMappingException.wrapWithPath(jsonMappingException, configRootNode, path
          .substring(1) + "/" + prop);
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

  public static <T> JsonNode getParam(T config, String pathAndProp)
      throws Exception {
    XmlMapper xmlMapper = xmlMapper();
    JsonNode configNode = xmlMapper.valueToTree(config);

    if (JsonPointer.compile(pathAndProp).equals(JsonPointer.compile("/"))) {
      return configNode;
    }

    JsonPointer pathPointer = JsonPointer.compile(pathAndProp);
    JsonNode pathNode = configNode.at(pathPointer);

    if (pathNode.isMissingNode() || pathNode.isNull()) {
      return null;
    }

    return pathNode;
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

  public static <T> void printYamlConfig(T config, Console console)
      throws IOException, JsonGenerationException, JsonMappingException {
    ObjectMapper objectMapper = yamlMapper();
    ObjectWriter objectWriter = objectMapper.writer();
    printConfig(config, console, objectWriter);
  }

  public static <T> void printXmlConfig(T config, Console console)
      throws IOException, JsonGenerationException, JsonMappingException {
    ObjectMapper objectMapper = xmlMapper();
    ObjectWriter objectWriter = objectMapper.writer();
    objectWriter = objectWriter.withRootName("config");
    printConfig(config, console, objectWriter);
  }

  private static <T> void printConfig(T config, Console console, ObjectWriter objectWriter)
      throws IOException, JsonGenerationException, JsonMappingException,
      UnsupportedEncodingException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayOutputStream, false, Charsets.UTF_8.name());
    objectWriter.writeValue(printStream, config);
    console.println(byteArrayOutputStream.toString(Charsets.UTF_8.name()));
  }

  public static <T> void printParamDescriptionFromConfigSchema(Class<T> configClass,
      ResourceBundle resourceBundle, Console console, int tabs)
      throws UnsupportedEncodingException, JsonMappingException {
    ObjectMapper objectMapper = mapper();
    DescriptionFactoryWrapper visitor = new DescriptionFactoryWrapper(
        resourceBundle, console, tabs);
    objectMapper.acceptJsonFormatVisitor(objectMapper.constructType(configClass), visitor);
    console.println("");
  }

  public static <T> void validateBean(T config) {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<T>> constraintViolations = validator.validate(config);
    if (!constraintViolations.isEmpty()) {
      IllegalArgumentException illegalArgumentException = transformConstraintsValidation(
          constraintViolations);
      throw illegalArgumentException;
    }
  }

  private static <T> IllegalArgumentException transformConstraintsValidation(
      Set<ConstraintViolation<T>> constraintViolations) {
    ConstraintViolation<T> constraintViolation = constraintViolations.iterator().next();
    Path path = constraintViolation.getPropertyPath();
    JsonPointer jsonPointer = toJsonPointer(path);
    return new IllegalArgumentException("Constraint validation errors at " + jsonPointer.toString()
        + ": "
        + constraintViolation.getMessage());
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
