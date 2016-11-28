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

package com.torodb.common.util;

import org.junit.Assert;
import org.junit.Test;

public class TextEscaperTest {

  private static final TextEscaper ESCAPER = TextEscaper.create(
      new TextEscaper.CharacterPredicate() {
    @Override
    public boolean apply(char character) {
      switch (character) {
        case ESCAPE_CHARACTER:
        case ZERO_CHARACTER:
        case COPY_ESCAPE_CHARACTER:
        case ROW_DELIMETER_CHARACTER:
        case COLUMN_DELIMETER_CHARACTER:
        case CARRIAGE_RETURN_CHARACTER:
          return true;
      }
      return false;
    }
  },
      new TextEscaper.CharacterPredicate() {
    @Override
    public boolean apply(char character) {
      switch (character) {
        case ESCAPE_CHARACTER:
        case COPY_ESCAPE_CHARACTER:
          return true;
      }
      return false;
    }
  },
      Escapable.values(), CopyEscapable.values());

  private final static char ESCAPE_CHARACTER = 1;
  private final static char ZERO_CHARACTER = 0;

  public enum Escapable implements TextEscaper.Escapable {
    ZERO(ZERO_CHARACTER, '0'),
    ESCAPE(ESCAPE_CHARACTER, '1');

    private final char character;
    private final char suffixCharacter;

    private Escapable(char character, char suffixCharacter) {
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
      return ESCAPE_CHARACTER;
    }
  }

  private static final char COPY_ESCAPE_CHARACTER = '\\';
  private static final char ROW_DELIMETER_CHARACTER = '\n';
  private static final char COLUMN_DELIMETER_CHARACTER = '\t';
  private static final char CARRIAGE_RETURN_CHARACTER = '\r';

  private enum CopyEscapable implements TextEscaper.Escapable {
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

  @Test
  public void testNoEscape() {
    String text = "Lorem ipsum dolor sit amet, "
        + "consectetur adipiscing elit, "
        + "sed eiusmod tempor incidunt ut labore et dolore magna aliqua. "
        + "Ut enim ad minim veniam, "
        + "quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. "
        + "Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
        + "Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    String encodedText = ESCAPER.escape(text);
    Assert.assertEquals("Lorem ipsum dolor sit amet, "
        + "consectetur adipiscing elit, "
        + "sed eiusmod tempor incidunt ut labore et dolore magna aliqua. "
        + "Ut enim ad minim veniam, "
        + "quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. "
        + "Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
        + "Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
        encodedText);
    String decodedText = ESCAPER.unescape(encodedText);
    Assert.assertEquals(
        text,
        decodedText);
  }

  @Test
  public void testSimpleEscape() {
    String text = "Lorem ipsum dolor sit amet, " + ZERO_CHARACTER
        + "consectetur adipiscing elit, "
        + "sed eiusmod tempor incidunt ut labore et dolore magna aliqua. "
        + "Ut enim ad minim veniam, "
        + "quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. "
        + "Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
        + "Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    String encodedText = ESCAPER.escape(text);
    Assert.assertEquals("Lorem ipsum dolor sit amet, " + ESCAPE_CHARACTER + "0"
        + "consectetur adipiscing elit, "
        + "sed eiusmod tempor incidunt ut labore et dolore magna aliqua. "
        + "Ut enim ad minim veniam, "
        + "quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. "
        + "Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
        + "Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
        encodedText);
    String decodedText = ESCAPER.unescape(encodedText);
    Assert.assertEquals(
        text,
        decodedText);
  }

  @Test
  public void testComplexEscape() {
    String text = "Lorem ipsum dolor sit amet, " + ZERO_CHARACTER
        + "consectetur adipiscing elit, " + ZERO_CHARACTER
        + "sed eiusmod tempor incidunt ut labore et dolore magna aliqua. " + ESCAPE_CHARACTER
        + "Ut enim ad minim veniam, " + ZERO_CHARACTER
        + "quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. "
        + ESCAPE_CHARACTER
        + "Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
        + "Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    String encodedText = ESCAPER.escape(text);
    Assert.assertEquals("Lorem ipsum dolor sit amet, " + ESCAPE_CHARACTER + "0"
        + "consectetur adipiscing elit, " + ESCAPE_CHARACTER + "0"
        + "sed eiusmod tempor incidunt ut labore et dolore magna aliqua. " + ESCAPE_CHARACTER + "1"
        + "Ut enim ad minim veniam, " + ESCAPE_CHARACTER + "0"
        + "quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. "
        + ESCAPE_CHARACTER + "1"
        + "Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
        + "Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
        encodedText);
    String decodedText = ESCAPER.unescape(encodedText);
    Assert.assertEquals(
        text,
        decodedText);
  }

  @Test
  public void testVeryComplexEscape() {
    String text = "Lorem ipsum dolor sit amet, " + COPY_ESCAPE_CHARACTER
        + "consectetur adipiscing elit, " + ZERO_CHARACTER
        + "sed eiusmod tempor incidunt ut labore et dolore magna aliqua. " + ESCAPE_CHARACTER
        + "Ut enim ad minim veniam, " + COLUMN_DELIMETER_CHARACTER
        + "quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. "
        + ESCAPE_CHARACTER
        + "Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
        + CARRIAGE_RETURN_CHARACTER
        + "Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    String encodedText = ESCAPER.escape(text);
    Assert.assertEquals("Lorem ipsum dolor sit amet, " + COPY_ESCAPE_CHARACTER
        + COPY_ESCAPE_CHARACTER
        + "consectetur adipiscing elit, " + ESCAPE_CHARACTER + "0"
        + "sed eiusmod tempor incidunt ut labore et dolore magna aliqua. " + ESCAPE_CHARACTER + "1"
        + "Ut enim ad minim veniam, " + COPY_ESCAPE_CHARACTER + COLUMN_DELIMETER_CHARACTER
        + "quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. "
        + ESCAPE_CHARACTER + "1"
        + "Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
        + COPY_ESCAPE_CHARACTER + CARRIAGE_RETURN_CHARACTER
        + "Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
        encodedText);
    String decodedText = ESCAPER.unescape(encodedText);
    Assert.assertEquals(
        text,
        decodedText);
  }
}
