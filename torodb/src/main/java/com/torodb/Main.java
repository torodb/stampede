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

package com.torodb;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.CharEncoding;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.eightkdata.mongowp.mongoserver.MongoServer;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.torodb.config.Config;
import com.torodb.config.backend.postgres.Postgres;
import com.torodb.di.BackendModule;
import com.torodb.di.ConfigModule;
import com.torodb.di.ConnectionModule;
import com.torodb.di.D2RModule;
import com.torodb.di.DbMetaInformationCacheModule;
import com.torodb.di.DbWrapperModule;
import com.torodb.di.ExecutorModule;
import com.torodb.di.ExecutorServiceModule;
import com.torodb.di.MongoLayerModule;
import com.torodb.torod.backend.db.postgresql.di.PostgreSQLModule;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.exceptions.TorodStartupException;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.util.xsd.XsdUtils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;

/**
 * ToroDB's entry point
 */
public class Main {
	public static void main(String[] args) throws Exception {
		final CliConfig cliConfig = new CliConfig();
		JCommander jCommander = new JCommander(cliConfig, args);
		jCommander.setColumnSize(Integer.MAX_VALUE);
		
		if (cliConfig.isHelp()) {
			String descriptionPrefix = "";
			
			for (ParameterDescription parameterDescription : jCommander.getParameters()) {
				if (parameterDescription.getParameter().getParameter() != null &&
						parameterDescription.getParameter().getParameter().descriptionKey() != null) {
					if (parameterDescription.getParameter().getParameter().descriptionKey().equals("param-description")) {
						descriptionPrefix = parameterDescription.getParameter().getParameter().description();
					}
				}
			}
			
			ResourceBundle bundle = extractParamDescriptionFromConfigSchema(descriptionPrefix);
			JCommander jCommanderForHelp = new JCommander(new CliConfig(), bundle);
			jCommanderForHelp.setColumnSize(Integer.MAX_VALUE);
			jCommanderForHelp.usage();
			System.exit(0);
		}
		
		Config config = new Config();
		JaxbAnnotationModule jaxbAnnotationModule = new JaxbAnnotationModule();
		ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
		yamlMapper.registerModule(jaxbAnnotationModule);
		XmlMapper xmlMapper = new XmlMapper();
		xmlMapper.registerModule(jaxbAnnotationModule);
		
		if (cliConfig.getConfFile() != null || cliConfig.getXmlConfFile() != null) {
			ObjectMapper mapper = null;
			InputStream validationInputStream = null;
			InputStream inputStream = null;
			if (cliConfig.getConfFile() != null) {
				mapper = yamlMapper;
				JsonNode configNode = yamlMapper.readTree(new File(cliConfig.getConfFile()));
				validationInputStream = openAsXml(configNode, xmlMapper);
				inputStream = new FileInputStream(cliConfig.getConfFile());
			} else 
			if (cliConfig.getXmlConfFile() != null) {
				mapper = xmlMapper;
				validationInputStream = new FileInputStream(cliConfig.getXmlConfFile());
				inputStream = new FileInputStream(cliConfig.getXmlConfFile());
			}
			
			if (inputStream != null) {
	            XsdUtils.validateWithXsd("/toroconfig.xsd", validationInputStream);
	            
				try {
		            config = mapper.readValue(inputStream, Config.class);
				} catch(JsonMappingException jsonMappingException) {
					handleJsonMappingException(jsonMappingException);
				}
			}
		}
		
		if (config.getBackend().isPostgresLike()) {
			Postgres postgres = config.getBackend().asPostgres();
			   
			File toroPass = new File(System.getProperty("user.home") + "/.toropass");
			if (toroPass.exists() && toroPass.canRead() && toroPass.isFile()) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						new FileInputStream(toroPass), Charset.forName("UTF-8")));
				String line;
				while ((line = br.readLine()) != null) {
				   String[] toroPassChunks = line.split(":");
				   if (toroPassChunks.length != 5) {
					   continue;
				   }
				   
				   if ((toroPassChunks[0].equals("*") || toroPassChunks[0].equals(postgres.getHost())) &&
						   (toroPassChunks[1].equals("*") || toroPassChunks[1].equals(String.valueOf(postgres.getPort()))) &&
						   (toroPassChunks[2].equals("*") || toroPassChunks[2].equals(postgres.getDatabase())) &&
						   (toroPassChunks[3].equals("*") || toroPassChunks[3].equals(postgres.getUser()))) {
					   postgres.setPassword(toroPassChunks[4]);
				   }
				}
				br.close();
			}

			if (cliConfig.askForPassword()) {
				postgres.setPassword(readPwd("Database user password:"));
			}
		}
		
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		switch(config.getGeneric().getLogLevel()) {
		case NONE:
			root.setLevel(Level.OFF);
			break;
		case INFO:
			root.setLevel(Level.INFO);
			break;
		case ERROR:
			root.setLevel(Level.ERROR);
			break;
		case WARNING:
			root.setLevel(Level.WARN);
			break;
		case DEBUG:
			root.setLevel(Level.DEBUG);
			break;
		case TRACE:
			root.setLevel(Level.ALL);
			break;
        }
		
		if (config.getGeneric().getLogFile() != null) {
			FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
			fileAppender.setFile(config.getGeneric().getLogFile());
			root.addAppender(fileAppender);
		}
		
		if (cliConfig.getParams() != null) {
			ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
			JaxbAnnotationModule module = new JaxbAnnotationModule();
			objectMapper.registerModule(module);
			JsonNode configNode = objectMapper.valueToTree(config);
			for (String paramPathValue : cliConfig.getParams()) {
				String[] paramPathAndValue = paramPathValue.split("=");
				String pathAndProp = paramPathAndValue[0].replace(".", "/");
				
				if (pathAndProp.startsWith("/")) {
					pathAndProp = pathAndProp.substring(1);
				}
				
				pathAndProp = "/" + pathAndProp;
				
				String value = paramPathAndValue[1];
				
				mergeParam(objectMapper, configNode, pathAndProp, value);
			}
			
			XsdUtils.validateWithXsd("/toroconfig.xsd", openAsXml(configNode, xmlMapper));

			try {
				config = objectMapper.treeToValue(configNode, Config.class);
			} catch(JsonMappingException jsonMappingException) {
				handleJsonMappingException(jsonMappingException);
			}
		}
		
		if (cliConfig.isPrintConfig()) {
			ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
			JaxbAnnotationModule module = new JaxbAnnotationModule();
			objectMapper.registerModule(module);
			objectMapper.configure(Feature.ALLOW_COMMENTS, true);
			objectMapper.configure(Feature.ALLOW_YAML_COMMENTS, true);
			objectMapper.writeValue(System.out, config);
			
			System.exit(0);
		}
		
		if (cliConfig.isPrintXmlConfig()) {
			ObjectMapper objectMapper = new XmlMapper();
			JaxbAnnotationModule module = new JaxbAnnotationModule();
			objectMapper.registerModule(module);
			objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
			objectMapper.configure(Feature.ALLOW_COMMENTS, true);
			objectMapper.configure(Feature.ALLOW_YAML_COMMENTS, true);
			objectMapper.writeValue(System.out, config);
			
			System.exit(0);
		}

		final ConfigMapper configMapper = ConfigMapper.create(config);

		Injector injector = Guice.createInjector(
				new BackendModule(configMapper),
				new PostgreSQLModule(),
				new ConfigModule(configMapper),
				new MongoLayerModule(configMapper),
				new DbWrapperModule(),
				new ExecutorModule(1000, 1000, 0.2),
				new DbMetaInformationCacheModule(),
				new D2RModule(),
				new ConnectionModule(),
                new ExecutorServiceModule()
		);

        try {
            final Torod torod = injector.getInstance(Torod.class);
            final MongoServer server = injector.getInstance(MongoServer.class);
            final DefaultBuildProperties buildProperties
                    = injector.getInstance(DefaultBuildProperties.class);
            final ReplCoordinator replCoord = injector.getInstance(ReplCoordinator.class);
            final Shutdowner shutdowner = injector.getInstance(Shutdowner.class);

            Thread shutdown = new Thread() {
                @Override
                public void run() {
                    shutdowner.shutdown();
                }
            };

            Runtime.getRuntime().addShutdownHook(shutdown);

            Thread serverThread = new Thread() {
                @Override
                public void run() {
                    JCommander.getConsole().println(
                            "Starting ToroDB v"
                            + buildProperties.getFullVersion()
                            + " listening on port " + configMapper.getDbPort()
                    );
                    Main.run(torod, server, replCoord);
                }
            };
            serverThread.start();
        }
        catch (ProvisionException pe) {
            String causeMessage;
            if (pe.getCause() != null) {
                causeMessage = pe.getCause().getMessage();
            }
            else {
                causeMessage = pe.getMessage();
            }
            JCommander.getConsole().println(causeMessage);
            System.exit(1);
        }
    }

	private static InputStream openAsXml(JsonNode configNode, XmlMapper xmlMapper)
			throws JsonProcessingException, UnsupportedEncodingException {
		InputStream validationInputStream;
		// Ugly hack to transform json node into xml and validate with XSD
		String xmlConfig = xmlMapper.writeValueAsString(configNode)
				.replace("<ObjectNode>", "<config xmlns=\"" + Config.TOROCONFIG_NAMESPACE + "\">")
				.replace("</ObjectNode>", "</config>");
		validationInputStream = new ByteArrayInputStream(xmlConfig.getBytes(CharEncoding.UTF_8));
		return validationInputStream;
	}

	private static void mergeParam(ObjectMapper objectMapper, JsonNode configRootNode, String pathAndProp, String value)
					throws IOException, JsonParseException, JsonMappingException {
		String path = pathAndProp.substring(0, pathAndProp.lastIndexOf("/"));
		String prop = pathAndProp.substring(pathAndProp.lastIndexOf("/") + 1);
		
		JsonPointer pathPointer = JsonPointer.compile(path); 
		JsonNode pathNode = configRootNode.at(pathPointer);
		
		if (pathNode.isMissingNode()) {
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
				JsonPointer missingPointer = missingPointers.get(missingPointerIndex);
				JsonPointer childOfMissingPointer = childOfMissingPointers.get(missingPointerIndex);
				
				JsonNode newNode = null;
				
				if (childOfMissingPointer == null || !childOfMissingPointer.last().mayMatchElement()) {
					newNode = JsonNodeFactory.instance.objectNode();
				} else {
					newNode = JsonNodeFactory.instance.arrayNode();
				}
				
				if (pathNode.isObject()) {
					((ObjectNode) pathNode).set(missingPointer.last().getMatchingProperty(), newNode);
				} else if (pathNode.isArray() && missingPointer.last().mayMatchElement()) {
					for (int index = ((ArrayNode) pathNode).size(); index < missingPointer.last().getMatchingIndex() + 1; index++) { 
						((ArrayNode) pathNode).add(newNode);
					}
				} else {
					throw new RuntimeException("Cannot set param " + pathAndProp + "=" + value);
				}
				
				pathNode = newNode;
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

	private static void handleJsonMappingException(JsonMappingException jsonMappingException) throws JsonMappingException {
		if (jsonMappingException.getPathReference().equals(Config.class.getName() + "[\"backend\"]")) {
			throw new RuntimeException("Just one backend must be specified", jsonMappingException);
		} else {
			throw jsonMappingException;
		}
	}

	private static void run(final Torod torod, final MongoServer server, final ReplCoordinator replCoord) {
		try {
			torod.start();
		} catch (TorodStartupException e) {
			LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).error(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}
        replCoord.startAsync();
        replCoord.awaitRunning();
		server.run();
	}

	private static String readPwd(String text) throws IOException {
		Console c = System.console();
		if (c == null) { // In Eclipse IDE
			System.out.print(text);
            InputStream in = System.in;
			int max = 50;
			byte[] b = new byte[max];

			int l = in.read(b);
			l--;// last character is \n
			if (l > 0) {
				byte[] e = new byte[l];
				System.arraycopy(b, 0, e, 0, l);
				return new String(e, Charset.forName("UTF-8"));
			} else {
				return null;
			}
		} else { // Outside Eclipse IDE
			return new String(c.readPassword(text));
		}
	}

	private static ResourceBundle extractParamDescriptionFromConfigSchema(String descriptionPrefix)
			throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException, SAXException, UnsupportedEncodingException {
		final Properties properties = new Properties();
		ResourceBundle bundle = new ResourceBundle() {

			@Override
			protected Object handleGetObject(String key) {
		        if (key == null) {
		            throw new NullPointerException();
		        }
		        return properties.getProperty(key);
		    }

			@Override
			public Enumeration<String> getKeys() {
				final Enumeration<Object> keys = properties.keys();
				return new Enumeration<String>() {
					@Override
					public boolean hasMoreElements() {
						return keys.hasMoreElements();
					}
					@Override
					public String nextElement() {
						Object nextElement = keys.nextElement();
						if (nextElement == null) {
							return null;
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
		
		paramPrintStream.println(descriptionPrefix);
		
		XsdUtils.extractDescriptionFromXsd("/toroconfig.xsd", Config.TOROCONFIG_NAMESPACE, paramPrintStream);
		
		properties.setProperty("param-description", paramByteArrayOutputStream.toString(CharEncoding.UTF_8));
		
		return bundle;
	}
}
