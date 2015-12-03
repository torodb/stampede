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

import java.util.List;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class CliConfig {
	
	@Parameter(names={"-h", "--help"}, description="Print help and exit.")
	private boolean help = false;
	@Parameter(names={"-l", "--print-config"}, description="Print the configuration in YAML format and exit.")
	private boolean printConfig = false;
	@Parameter(names={"-lx", "--print-xml-config"}, description="Print the configuration in XML format and exit.")
	private boolean printXmlConfig = false;
	@Parameter(names={"-c","--conf"}, description="Configuration file in YAML format.")
	private String confFile;
	@Parameter(names={"-x","--xml-conf"}, description="Configuration file in XML format.")
	private String xmlConfFile;
	@Parameter(names={"-p","--param"}, description="Specify a configuration parameter using <path>=<value> syntax.\n\n"
			+ "<path> follow JSON pointer format (http://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-03) with exception that you can replace '/' characters for '.' characters and omit the first '/' character.\n"
			+ "<value> have the same syntax as values in YAML 1.1 format (http://yaml.org/spec/1.1/).\n"
			+ "Possible paths are:\n", descriptionKey="param-description",validateValueWith=ParamValueValidator.class)
	private List<String> params;
	@Parameter(names={"-W", "--ask-for-password"}, description="Force input of PostgreSQL's database user password.")
	private boolean askForPassword = false;

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
	public String getXmlConfFile() {
		return xmlConfFile;
	}
	public List<String> getParams() {
		return params;
	}
	public boolean isAskForPassword() {
		return askForPassword;
	}
	public boolean askForPassword() {
    	return askForPassword;
    }
	
	public static class ParamValueValidator implements IValueValidator<List<String>> {
		@Override
		public void validate(String name, List<String> value) throws ParameterException {
			for (String param : value) {
				if (param.split("=").length != 2) {
					throw new ParameterException("Wrong parameter format: " + param);
				}
			}
		}
	}
}
