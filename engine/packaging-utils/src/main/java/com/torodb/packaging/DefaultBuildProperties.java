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

package com.torodb.packaging;

import com.google.common.io.Resources;
import com.torodb.core.BuildProperties;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.Immutable;
import javax.inject.Singleton;

@Immutable
@Singleton
public class DefaultBuildProperties implements BuildProperties {

  public static final String BUILD_PROPERTIES_FILE = "ToroDB.build.properties";
  public static final Pattern FULL_VERSION_PATTERN = Pattern.compile(
      "(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:-(.+))?");

  private final String fullVersion;
  private final int majorVersion;
  private final int minorVersion;
  private final int subVersion;
  private final String extraVersion;
  private final Instant buildTime;
  private final String gitCommitId;
  private final String gitBranch;
  private final String gitRemoteOriginUrl;
  private final String javaVersion;
  private final String javaVendor;
  private final String javaVmSpecificationVersion;
  private final String javaVmVersion;
  private final String osName;
  private final String osArch;
  private final String osVersion;

  public DefaultBuildProperties() {
    this(BUILD_PROPERTIES_FILE);
  }

  public DefaultBuildProperties(String propertiesFile) {
    PropertiesConfiguration properties;
    try {
      properties = new PropertiesConfiguration(Resources.getResource(propertiesFile));
    } catch (ConfigurationException e) {
      throw new RuntimeException("Cannot read build properties file '" + propertiesFile + "'");
    }

    fullVersion = properties.getString("version");
    Matcher matcher = FULL_VERSION_PATTERN.matcher(fullVersion);
    if (!matcher.matches()) {
      throw new RuntimeException("Invalid version string '" + fullVersion + "'");
    }
    majorVersion = Integer.parseInt(matcher.group(1));
    minorVersion = Integer.parseInt(matcher.group(2));
    subVersion = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
    extraVersion = matcher.group(4);

    // DateUtils.parseDate may be replaced by SimpleDateFormat if using Java7
    try {
      buildTime = Instant.parse(properties.getString("buildTimestamp"));
    } catch (DateTimeParseException e) {
      throw new RuntimeException("buildTimestamp property not in ISO8601 format", e);
    }

    gitCommitId = properties.getString("gitCommitId");
    gitBranch = properties.getString("gitBranch");
    gitRemoteOriginUrl = properties.getString("gitRemoteOriginURL");

    javaVersion = properties.getString("javaVersion");
    javaVendor = properties.getString("javaVendor");
    javaVmSpecificationVersion = properties.getString("javaVMSpecificationVersion");
    javaVmVersion = properties.getString("javaVMVersion");

    osName = properties.getString("osName");
    osArch = properties.getString("osArch");
    osVersion = properties.getString("osVersion");
  }

  @Override
  public String getFullVersion() {
    return fullVersion;
  }

  @Override
  public int getMajorVersion() {
    return majorVersion;
  }

  @Override
  public int getMinorVersion() {
    return minorVersion;
  }

  @Override
  public int getSubVersion() {
    return subVersion;
  }

  @Override
  public String getExtraVersion() {
    return extraVersion;
  }

  @Override
  public Instant getBuildTime() {
    return buildTime;
  }

  @Override
  public String getGitCommitId() {
    return gitCommitId;
  }

  @Override
  public String getGitBranch() {
    return gitBranch;
  }

  public String getGitRemoteOriginUrl() {
    return gitRemoteOriginUrl;
  }

  @Override
  public String getJavaVersion() {
    return javaVersion;
  }

  @Override
  public String getJavaVendor() {
    return javaVendor;
  }

  @Override
  public String getJavaVmSpecificationVersion() {
    return javaVmSpecificationVersion;
  }

  @Override
  public String getJavaVmVersion() {
    return javaVmVersion;
  }

  @Override
  public String getOsName() {
    return osName;
  }

  @Override
  public String getOsArch() {
    return osArch;
  }

  @Override
  public String getOsVersion() {
    return osVersion;
  }
}
