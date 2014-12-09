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

import com.google.common.io.Resources;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.time.DateUtils;

import javax.annotation.concurrent.Immutable;
import javax.inject.Singleton;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
@Immutable
@Singleton
public class BuildProperties {
    public static final String BUILD_PROPERTIES_FILE = "ToroDB.build.properties";
    public static final String ISO8601_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssZZ";
    public static final Pattern FULL_VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:-(.+))?");

    private final String fullVersion;
    private final int majorVersion;
    private final int minorVersion;
    private final int subVersion;
    private final String extraVersion;
    private final Date buildTime;
    private final String gitCommitId;
    private final String gitBranch;
    private final String gitRemoteOriginUrl;
    private final String javaVersion;
    private final String javaVendor;
    private final String javaVMSpecificationVersion;
    private final String javaVMVersion;
    private final String osName;
    private final String osArch;
    private final String osVersion;

    BuildProperties() {
        this(BUILD_PROPERTIES_FILE);
    }

    BuildProperties(String propertiesFile) {
        PropertiesConfiguration properties;
        try {
            properties = new PropertiesConfiguration(Resources.getResource(propertiesFile));
        } catch (ConfigurationException e) {
            throw new RuntimeException("Cannot read build properties file '" + propertiesFile + "'");
        }

        fullVersion = properties.getString("version");
        Matcher matcher = FULL_VERSION_PATTERN.matcher(fullVersion);
        if(! matcher.matches()) {
            throw new RuntimeException("Invalid version string '" + fullVersion + "'");
        }
        majorVersion = Integer.parseInt(matcher.group(1));
        minorVersion = Integer.parseInt(matcher.group(2));
        subVersion = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
        extraVersion = matcher.group(4);

        // DateUtils.parseDate may be replaced by SimpleDateFormat if using Java7
        try {
            buildTime = DateUtils.parseDate(properties.getString("build.timestamp"), new String[]{ ISO8601_FORMAT_STRING });
        } catch (ParseException e) {
            throw new RuntimeException("build.timestamp property not in ISO8601 format");
        }

        gitCommitId = properties.getString("git.commit.id");
        gitBranch = properties.getString("git.branch");
        gitRemoteOriginUrl = properties.getString("git.remote.origin.url");

        javaVersion = properties.getString("java.version");
        javaVendor = properties.getString("java.vendor");
        javaVMSpecificationVersion = properties.getString("java.vm.specification.version");
        javaVMVersion = properties.getString("java.vm.version");

        osName = properties.getString("os.name");
        osArch = properties.getString("os.arch");
        osVersion = properties.getString("os.version");
    }

    public String getFullVersion() {
        return fullVersion;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public int getSubVersion() {
        return subVersion;
    }

    public String getExtraVersion() {
        return extraVersion;
    }

    public long getBuildTime() {
        return buildTime.getTime();
    }

    public String getGitCommitId() {
        return gitCommitId;
    }

    public String getGitBranch() {
        return gitBranch;
    }

    public String getGitRemoteOriginUrl() {
        return gitRemoteOriginUrl;
    }

    public String getJavaVersion() {
        return javaVersion;
    }

    public String getJavaVendor() {
        return javaVendor;
    }

    public String getJavaVMSpecificationVersion() {
        return javaVMSpecificationVersion;
    }

    public String getJavaVMVersion() {
        return javaVMVersion;
    }

    public String getOsName() {
        return osName;
    }

    public String getOsArch() {
        return osArch;
    }

    public String getOsVersion() {
        return osVersion;
    }
}
