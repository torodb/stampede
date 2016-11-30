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

package com.torodb.backend.postgresql.converters.util;

import static com.torodb.backend.postgresql.converters.util.EscapableConstants.SQL_ESCAPE_CHARACTER;
import static com.torodb.backend.postgresql.converters.util.EscapableConstants.ZERO_CHARACTER;

import com.torodb.common.util.TextEscaper;

public class SqlEscaper extends TextEscaper {

  public static final SqlEscaper INSTANCE = new SqlEscaper();

  protected SqlEscaper() {
    super(new TextEscaper.CharacterPredicate() {
      @Override
      public boolean apply(char character) {
        switch (character) {
          case SQL_ESCAPE_CHARACTER:
          case ZERO_CHARACTER:
            return true;
          default:
            return false;
        }
      }
    },
        new TextEscaper.CharacterPredicate() {
      @Override
      public boolean apply(char character) {
        switch (character) {
          case SQL_ESCAPE_CHARACTER:
            return true;
          default:
            return false;
        }
      }
    },
        SqlEscapable.values());
  }

  public enum SqlEscapable implements Escapable {
    ZERO(ZERO_CHARACTER, '0'),
    ESCAPE(SQL_ESCAPE_CHARACTER, '1');

    private final char character;
    private final char suffixCharacter;

    private SqlEscapable(char character, char suffixCharacter) {
      this.character = character;
      this.suffixCharacter = suffixCharacter;
    }

    @Override
    public char getCharacter() {
      return character;
    }

    @Override
    public char getSuffixCharacter() {
      return suffixCharacter;
    }

    @Override
    public char getEscapeCharacter() {
      return SQL_ESCAPE_CHARACTER;
    }
  }

}
