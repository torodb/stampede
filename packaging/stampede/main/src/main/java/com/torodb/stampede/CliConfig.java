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

package com.torodb.stampede;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.IParameterSplitter;
import com.torodb.packaging.config.model.backend.BackendImplementation;
import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.stampede.config.model.backend.Backend;

public class CliConfig {
	
	@Parameter(names={"-h", "--help"}, descriptionKey="cli.help")
	private boolean help = false;
	@Parameter(names={"-l", "--print-config"}, descriptionKey="cli.print-config")
	private boolean printConfig = false;
	@Parameter(names={"-lx", "--print-xml-config"}, descriptionKey="cli.print-xml-config")
	private boolean printXmlConfig = false;
	@Parameter(names={"-hp", "--help-param"}, descriptionKey="cli.help-param")
	private boolean helpParam = false;
    @Parameter(names={"-lp","--print-param"}, descriptionKey="cli.print-param")
    private String printParam;
	@Parameter(names={"-c","--conf"}, descriptionKey="cli.conf")
	private String confFile;
	@Parameter(names={"-x","--xml-conf"}, descriptionKey="cli.xml-conf")
	private String xmlConfFile;
	@Parameter(names={"-W", "--ask-for-password"}, descriptionKey="cli.ask-for-password")
	private boolean askForPassword = false;
    @Parameter(names={"-b","--backend"}, descriptionKey="cli.backend",validateValueWith=BackendValueValidator.class)
    private String backend;
	@Parameter(names={"-p","--param"}, descriptionKey="cli.param",validateValueWith=ParamListValueValidator.class,
			splitter=NoParameterSplitter.class)
	private List<String> params;
    @Parameter(names={"--log-level"}, descriptionKey="config.generic.logLevel")
    private String logLevel;
    @Parameter(names={"--log4j2-file"}, descriptionKey="config.generic.log4j2File")
    private String log4j2File;
    @Parameter(names={"--connection-pool-timeout"}, descriptionKey="config.generic.connectionPoolTimeout")
    private String connectionPoolTimeout;
    @Parameter(names={"--connection-pool-size"}, descriptionKey="config.generic.connectionPoolSize")
    private String connectionPoolSize;
    @Parameter(names={"--reserved-read-pool-size"}, descriptionKey="config.generic.reservedReadPoolSize")
    private String reservedReadPoolSize;
    @Parameter(names={"--enable-metrics"}, descriptionKey="config.generic.enableMetrics")
    private Boolean metricsEnabled;

    @Parameter(names={"--repl-set-name"}, descriptionKey="config.mongo.replication.replSetName")
    private String replSetName;
    @Parameter(names={"--role"}, descriptionKey="config.mongo.replication.role")
    private String role;
    @Parameter(names={"--sync-source"}, descriptionKey="config.mongo.replication.syncSource")
    private String syncSource;
    @Parameter(names={"--enable-ssl"}, descriptionKey="config.mongo.replication.enableSsl")
    private Boolean sslEnabled;
    @Parameter(names={"--ssl-allow-invalid-hostnames"}, descriptionKey="config.mongo.replication.ssl.allowInvalidHostnames")
    private String sslAllowInvalidHostnames;
    @Parameter(names={"--ssl-fips-mode"}, descriptionKey="config.mongo.replication.ssl.fipsMode")
    private String sslFipsMode;
    @Parameter(names={"--ssl-ca-file"}, descriptionKey="config.mongo.replication.ssl.caFile")
    private String sslCaFile;
    @Parameter(names={"--ssl-trust-store-file"}, descriptionKey="config.mongo.replication.ssl.trustStoreFile")
    private String sslTrustStoreFile;
    @Parameter(names={"--ssl-trust-store-password"}, descriptionKey="config.mongo.replication.ssl.trustStorePassword")
    private String sslTrustStorePassword;
    @Parameter(names={"--ssl-key-store-file"}, descriptionKey="config.mongo.replication.ssl.keyStoreFile")
    private String sslKeyStoreFile;
    @Parameter(names={"--ssl-key-store-password"}, descriptionKey="config.mongo.replication.ssl.keyStorePassword")
    private String sslKeyStorePassword;
    @Parameter(names={"--ssl-key-password"}, descriptionKey="config.mongo.replication.ssl.keyPassword")
    private String sslKeyPassword;
    @Parameter(names={"--auth-mode"}, descriptionKey="config.mongo.replication.auth.mode")
    private String authMode;
    @Parameter(names={"--auth-user"}, descriptionKey="config.mongo.replication.auth.user")
    private String authUser;
    @Parameter(names={"--auth-source"}, descriptionKey="config.mongo.replication.auth.source")
    private String authSource;
    @Parameter(names={"--gssapi-service-name"}, descriptionKey="config.mongo.replication.auth.gssapiServiceName")
    private String gssapiServiceName;
    @Parameter(names={"--gssapi-host-name"}, descriptionKey="config.mongo.replication.auth.gssapiHostName")
    private String gssapiHostName;
    @Parameter(names={"--gssapi-subject"}, descriptionKey="config.mongo.replication.auth.gssapiSubject")
    private String gssapiSubject;
    @Parameter(names={"--gssapi-sasl-client-properties"}, descriptionKey="config.mongo.replication.auth.gssapiSaslClientProperties")
    private String gssapiSaslClientProperties;
    @Parameter(names={"--cursor-timeout"}, descriptionKey="config.mongo.cursorTimeout")
    private String cursorTimeout;
    @Parameter(names={"--mongopass-file"}, descriptionKey="config.mongo.mongopassFile")
    private String mongopassFile;

    @Parameter(names={"--backend-host"}, descriptionKey="config.backend.postgres.host")
    private String backendHost;
    @Parameter(names={"--backend-port"}, descriptionKey="config.backend.postgres.port")
    private String backendPort;
    @Parameter(names={"--backend-database"}, descriptionKey="config.backend.postgres.database")
    private String backendDatabase;
    @Parameter(names={"--backend-user"}, descriptionKey="config.backend.postgres.user")
    private String backendUser;
    @Parameter(names={"--toropass-file"}, descriptionKey="config.backend.postgres.toropassFile")
    private String toropassFile;
    @Parameter(names={"--application-name"}, descriptionKey="config.backend.postgres.applicationName")
    private String applicationName;

	public boolean isHelp() {
		return help;
	}
	public boolean isPrintConfig() {
		return printConfig;
	}
	public boolean isPrintXmlConfig() {
		return printXmlConfig;
	}
    public boolean isPrintParam() {
        return printParam != null;
    }
    public String getPrintParamPath() {
        return printParam;
    }
	public String getConfFile() {
		return confFile;
	}
	public boolean hasConfFile() {
		return confFile != null;
	}
	public InputStream getConfInputStream() throws Exception {
		return new FileInputStream(confFile);
	}
	public String getXmlConfFile() {
		return xmlConfFile;
	}
	public boolean hasXmlConfFile() {
		return xmlConfFile != null;
	}
	public InputStream getXmlConfInputStream() throws Exception {
		return new FileInputStream(xmlConfFile);
	}
	public boolean isHelpParam() {
		return helpParam;
	}
	public boolean isAskForPassword() {
		return askForPassword;
	}
    public List<String> getParams() {
        return params;
    }
    public void addParam(String path, String value) {
        if (params == null) {
            params = new ArrayList<>();
        }
        params.add(path + "=" + value);
    }
    public String getBackend() {
        return backend;
    }
    
    public String getLogLevel() {
        return logLevel;
    }
    public String getLog4j2File() {
        return log4j2File;
    }
    public String getConnectionPoolTimeout() {
        return connectionPoolTimeout;
    }
    public String getConnectionPoolSize() {
        return connectionPoolSize;
    }
    public String getReservedReadPoolSize() {
        return reservedReadPoolSize;
    }
    public Boolean getMetricsEnabled() {
        return metricsEnabled;
    }
    public String getReplSetName() {
        return replSetName;
    }
    public String getRole() {
        return role;
    }
    public String getSyncSource() {
        return syncSource;
    }
    public Boolean getSslEnabled() {
        return sslEnabled;
    }
    public String getSslAllowInvalidHostnames() {
        return sslAllowInvalidHostnames;
    }
    public String getSslFipsMode() {
        return sslFipsMode;
    }
    public String getSslCaFile() {
        return sslCaFile;
    }
    public String getSslTrustStoreFile() {
        return sslTrustStoreFile;
    }
    public String getSslTrustStorePassword() {
        return sslTrustStorePassword;
    }
    public String getSslKeyStoreFile() {
        return sslKeyStoreFile;
    }
    public String getSslKeyStorePassword() {
        return sslKeyStorePassword;
    }
    public String getSslKeyPassword() {
        return sslKeyPassword;
    }
    public String getAuthMode() {
        return authMode;
    }
    public String getAuthUser() {
        return authUser;
    }
    public String getAuthSource() {
        return authSource;
    }
    public String getGssapiServiceName() {
        return gssapiServiceName;
    }
    public String getGssapiHostName() {
        return gssapiHostName;
    }
    public String getGssapiSubject() {
        return gssapiSubject;
    }
    public String getGssapiSaslClientProperties() {
        return gssapiSaslClientProperties;
    }
    public String getCursorTimeout() {
        return cursorTimeout;
    }
    public String getMongopassFile() {
        return mongopassFile;
    }
    public String getBackendHost() {
        return backendHost;
    }
    public String getBackendPort() {
        return backendPort;
    }
    public String getBackendDatabase() {
        return backendDatabase;
    }
    public String getBackendUser() {
        return backendUser;
    }
    public String getToropassFile() {
        return toropassFile;
    }
    public String getApplicationName() {
        return applicationName;
    }
    
    public void addParams() {
        if (logLevel != null) {
            addParam("/generic/logLevel", logLevel);
        }
        if (log4j2File != null) {
            addParam("/generic/log4j2File", log4j2File);
        }
        if (connectionPoolTimeout != null) {
            addParam("/generic/connectionPoolTimeout", connectionPoolTimeout);
        }
        if (connectionPoolSize != null) {
            addParam("/generic/connectionPoolSize", connectionPoolSize);
        }
        if (reservedReadPoolSize != null) {
            addParam("/generic/reservedReadPoolSize", reservedReadPoolSize);
        }
        if (metricsEnabled != null) {
            addParam("/generic/metricsEnabled", metricsEnabled?"true":"false");
        }
        if (replSetName != null) {
            addParam("/replication/replSetName", replSetName);
        }
        if (role != null) {
            addParam("/replication/role", role);
        }
        if (syncSource != null) {
            addParam("/replication/syncSource", syncSource);
        }
        if (sslEnabled != null) {
            addParam("/replication/ssl/enabled", sslEnabled?"true":"false");
        }
        if (sslAllowInvalidHostnames != null) {
            addParam("/replication/ssl/allowInvalidHostnames", sslAllowInvalidHostnames);
        }
        if (sslFipsMode != null) {
            addParam("/replication/ssl/fipsMode", sslFipsMode);
        }
        if (sslCaFile != null) {
            addParam("/replication/ssl/caFile", sslCaFile);
        }
        if (sslTrustStoreFile != null) {
            addParam("/replication/ssl/trustStoreFile", sslTrustStoreFile);
        }
        if (sslTrustStorePassword != null) {
            addParam("/replication/ssl/trustStorePassword", sslTrustStorePassword);
        }
        if (sslKeyStoreFile != null) {
            addParam("/replication/ssl/keyStoreFile", sslKeyStoreFile);
        }
        if (sslKeyStorePassword != null) {
            addParam("/replication/ssl/keyStorePassword", sslKeyStorePassword);
        }
        if (sslKeyPassword != null) {
            addParam("/replication/ssl/keyPassword", sslKeyPassword);
        }
        if (authMode != null) {
            addParam("/replication/auth/mode", authMode);
        }
        if (authUser != null) {
            addParam("/replication/auth/user", authUser);
        }
        if (authSource != null) {
            addParam("/replication/auth/source", authSource);
        }
        if (gssapiServiceName != null) {
            addParam("/replication/auth/gssapiServiceName", gssapiServiceName);
        }
        if (gssapiHostName != null) {
            addParam("/replication/auth/gssapiHostName", gssapiHostName);
        }
        if (gssapiSubject != null) {
            addParam("/replication/auth/gssapiSubject", gssapiSubject);
        }
        if (gssapiSaslClientProperties != null) {
            addParam("/replication/auth/gssapiSaslClientProperties", gssapiSaslClientProperties);
        }
        if (cursorTimeout != null) {
            addParam("/replication/cursorTimeout", cursorTimeout);
        }
        if (mongopassFile != null) {
            addParam("/replication/mongopassFile", mongopassFile);
        }
        String backend = this.backend != null ? this.backend : "postgres";
        if (backendHost != null) {
            addParam("/backend/" + backend + "/host", backendHost);
        }
        if (backendPort != null) {
            addParam("/backend/" + backend + "/port", backendPort);
        }
        if (backendDatabase != null) {
            addParam("/backend/" + backend + "/database", backendDatabase);
        }
        if (backendUser != null) {
            addParam("/backend/" + backend + "/user", backendUser);
        }
        if (toropassFile != null) {
            addParam("/backend/" + backend + "/toropassFile", toropassFile);
        }
        if (applicationName != null) {
            addParam("/backend/" + backend + "/applicationName", applicationName);
        }
    }

    public static class ParamListValueValidator implements IValueValidator<List<String>> {
        @Override
        public void validate(String name, List<String> value) throws ParameterException {
            for (String param : value) {
                if (param.indexOf('=') == -1) {
                    throw new ParameterException("Wrong parameter format: " + param);
                }
            }
        }
    }
    
    public static Class<? extends BackendImplementation> getBackendClass(String backend) {
        backend = backend.toLowerCase(Locale.US);
        
        for (Class<? extends BackendImplementation> backendClass : Backend.BACKEND_CLASSES.values()) {
            String backendClassLabel = backendClass.getSimpleName().toLowerCase(Locale.US);
            if (backend.equals(backendClassLabel)) {
                return backendClass;
            }
        }
        
        return null;
    }
    public static class BackendValueValidator implements IValueValidator<String> {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (value != null && 
                    (getBackendClass(value) == null || 
                    getBackendClass(value) == Derby.class)) {
                List<String> possibleValues = new ArrayList<>();
                for (Class<? extends BackendImplementation> backendClass : Backend.BACKEND_CLASSES.values()) {
                    if (backendClass == Derby.class) {
                        continue;
                    }
                    possibleValues.add(backendClass.getSimpleName().toLowerCase(Locale.US));
                }
                throw new ParameterException("Unknown backend: " + value + " (possible values are: " + possibleValues + ")");
            }
        }
    }
	
	public static class NoParameterSplitter implements IParameterSplitter {
		@Override
		public List<String> split(String value) {
			return Arrays.asList(new String[] { value });
		}
	}
}
