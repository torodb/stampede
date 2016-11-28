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

package com.torodb.stampede;

import static com.torodb.packaging.config.util.ConfigUtils.validateBean;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.stampede.config.model.Config;
import com.torodb.stampede.config.model.backend.Backend;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class CliConfigUtils {

  private CliConfigUtils() {
  }

  public static Config readConfig(CliConfig cliConfig) throws FileNotFoundException,
      JsonProcessingException,
      IOException, JsonParseException, IllegalArgumentException, Exception {
    try {
      return uncatchedReadConfig(cliConfig);
    } catch (JsonMappingException jsonMappingException) {
      throw ConfigUtils.transformJsonMappingException(jsonMappingException);
    }
  }

  private static Config uncatchedReadConfig(final CliConfig cliConfig) throws Exception {
    ObjectMapper objectMapper = ConfigUtils.mapper();

    Config defaultConfig = new Config();
    ObjectNode configNode = (ObjectNode) objectMapper.valueToTree(defaultConfig);

    if (cliConfig.hasConfFile() || cliConfig.hasXmlConfFile()) {
      ObjectMapper mapper = null;
      InputStream inputStream = null;
      if (cliConfig.hasConfFile()) {
        mapper = ConfigUtils.yamlMapper();
        inputStream = cliConfig.getConfInputStream();
      } else if (cliConfig.hasXmlConfFile()) {
        mapper = ConfigUtils.xmlMapper();
        inputStream = cliConfig.getXmlConfInputStream();
      }

      if (inputStream != null) {
        Config config = mapper.readValue(inputStream, Config.class);
        configNode = mapper.valueToTree(config);
      }
    }

    if (cliConfig.getBackend() != null) {
      Backend backend = new Backend(
          CliConfig.getBackendClass(cliConfig.getBackend()).newInstance());
      ObjectNode backendNode = (ObjectNode) objectMapper.valueToTree(backend);
      configNode.set("backend", backendNode);
    }

    if (cliConfig.getParams() != null) {
      YAMLMapper yamlMapper = ConfigUtils.yamlMapper();
      for (String paramPathValue : cliConfig.getParams()) {
        int paramPathValueSeparatorIndex = paramPathValue.indexOf('=');
        String pathAndProp = paramPathValue.substring(0, paramPathValueSeparatorIndex);

        if (pathAndProp.startsWith("/")) {
          pathAndProp = pathAndProp.substring(1);
        }

        pathAndProp = "/" + pathAndProp;

        String value = paramPathValue.substring(paramPathValueSeparatorIndex + 1);

        configNode = ConfigUtils.mergeParam(yamlMapper, configNode, pathAndProp, value);
      }
    }

    Config config = objectMapper.treeToValue(configNode, Config.class);

    validateBean(config);

    return config;
  }
}
