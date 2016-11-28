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

import static com.torodb.backend.postgresql.converters.util.EscapableConstants.CARRIAGE_RETURN_CHARACTER;
import static com.torodb.backend.postgresql.converters.util.EscapableConstants.COLUMN_DELIMETER_CHARACTER;
import static com.torodb.backend.postgresql.converters.util.EscapableConstants.COPY_ESCAPE_CHARACTER;
import static com.torodb.backend.postgresql.converters.util.EscapableConstants.ROW_DELIMETER_CHARACTER;
import static com.torodb.backend.postgresql.converters.util.EscapableConstants.SQL_ESCAPE_CHARACTER;
import static com.torodb.backend.postgresql.converters.util.EscapableConstants.ZERO_CHARACTER;

import com.torodb.backend.postgresql.converters.util.SqlEscaper.SqlEscapable;
import com.torodb.common.util.TextEscaper;

public class CopyEscaper extends TextEscaper {

  public static final CopyEscaper INSTANCE = new CopyEscaper();

  protected CopyEscaper() {
    super(new TextEscaper.CharacterPredicate() {
      @Override
      public boolean apply(char character) {
        switch (character) {
          case SQL_ESCAPE_CHARACTER:
          case ZERO_CHARACTER:
          case COPY_ESCAPE_CHARACTER:
          case ROW_DELIMETER_CHARACTER:
          case COLUMN_DELIMETER_CHARACTER:
          case CARRIAGE_RETURN_CHARACTER:
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
          case COPY_ESCAPE_CHARACTER:
            return true;
          default:
            return false;
        }
      }
    },
        SqlEscapable.values(), CopyEscapable.values());
  }

  private enum CopyEscapable implements Escapable {
    ROW_DELIMETER(ROW_DELIMETER_CHARACTER, ROW_DELIMETER_CHARACTER),
    COLUMN_DELIMETER(COLUMN_DELIMETER_CHARACTER, COLUMN_DELIMETER_CHARACTER),
    CARRIAGE_RETURN(CARRIAGE_RETURN_CHARACTER, CARRIAGE_RETURN_CHARACTER),
    COPY_ESCAPE(COPY_ESCAPE_CHARACTER, COPY_ESCAPE_CHARACTER);

    private final char character;
    private final char suffixCharacter;

    private CopyEscapable(char character, char suffixCharacter) {
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
      return COPY_ESCAPE_CHARACTER;
    }
  }

}
