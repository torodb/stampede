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
import com.torodb.config.model.backend.Backend;
import com.torodb.config.model.backend.BackendImplementation;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.mysql.MySQL;
import com.torodb.config.model.backend.postgres.Postgres;

public class CliConfig {
	
	@Parameter(names={"-h", "--help"}, descriptionKey="help")
	private boolean help = false;
	@Parameter(names={"-l", "--print-config"}, descriptionKey="print-config")
	private boolean printConfig = false;
	@Parameter(names={"-lx", "--print-xml-config"}, descriptionKey="print-xml-config")
	private boolean printXmlConfig = false;
	@Parameter(names={"-hp", "--help-param"}, descriptionKey="help-param")
	private boolean helpParam = false;
	@Parameter(names={"-c","--conf"}, descriptionKey="conf")
	private String confFile;
	@Parameter(names={"-x","--xml-conf"}, descriptionKey="xml-conf")
	private String xmlConfFile;
	@Parameter(names={"-W", "--ask-for-password"}, descriptionKey="ask-for-password")
	private boolean askForPassword = false;
    @Parameter(names={"-b","--backend"}, descriptionKey="backend",validateValueWith=BackendValueValidator.class)
    private String backend;
	@Parameter(names={"-p","--param"}, descriptionKey="param",validateValueWith=ParamValueValidator.class,
			splitter=NoParameterSplitter.class)
	private List<String> params;

	public boolean isHelp() {
		return help;
	}
	public boolean isPrintConfig() {
		return printConfig;
	}
	public boolean isPrintXmlConfig() {
		return printXmlConfig;
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
    public String getBackend() {
        return backend;
    }
    
    public static class ParamValueValidator implements IValueValidator<List<String>> {
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
        
        for (Class<? extends BackendImplementation> backendClass : Backend.BACKEND_CLASSES) {
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
            if (value != null && getBackendClass(value) == null) {
                List<String> possibleValues = new ArrayList<>();
                for (Class<? extends BackendImplementation> backendClass : Backend.BACKEND_CLASSES) {
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
