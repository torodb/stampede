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

import java.util.regex.Pattern;

public class SimpleRegExpDecoder {

  private static final char DELIMITER = '\\';
  private static final char ANY_CHARACTER = '*';

  /**
   * Accept a simplified regular expression that replace character '*' with ".*". Any other
   * character will be quoted and character '\\' can be used to protect any character. For example:
   * "mine_and_your" >> "\\Qmine_and_your\\E" "mine_*_your" >> "\\Qmine_\\E.*\\Q_your\\E"
   * "mine_\\*_your" >> "\\Qmine_*_your\\E" "min\\e_\\\\*_yo\\ur" >> "\\Qmine_\\*_your\\E"
   *
   * @param simpleRegExp the simple regular expression
   * @return the pattern resulting from converting the simple regular expression to a regular
   *         expression
   */
  public static Pattern decode(String simpleRegExp) {
    StringBuilder resultPatternBuilder = new StringBuilder();

    boolean quoted = false;
    final int length = simpleRegExp.length();

    for (int index = 0; index < length; index++) {
      char simpleRegExpChar = simpleRegExp.charAt(index);
      if (simpleRegExpChar == ANY_CHARACTER) {
        if (quoted) {
          resultPatternBuilder.append("\\E");
          quoted = false;
        }
        resultPatternBuilder.append(".*");
      } else {
        if (!quoted) {
          resultPatternBuilder.append("\\Q");
          quoted = true;
        }
        if (simpleRegExpChar == DELIMITER) {
          index++;
          simpleRegExpChar = simpleRegExp.charAt(index);
        }
        resultPatternBuilder.append(simpleRegExpChar);
      }
    }

    if (quoted) {
      resultPatternBuilder.append("\\E");
    }

    return Pattern.compile(resultPatternBuilder.toString());
  }

}
