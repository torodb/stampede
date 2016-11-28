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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;

public class TextEscaper {

  public interface CharacterPredicate {

    public boolean apply(char character);
  }

  public interface Escapable {

    public char getCharacter();

    public char getSuffixCharacter();

    public char getEscapeCharacter();
  }

  public static TextEscaper create(CharacterPredicate isEscapable,
      CharacterPredicate isEscapeCharacter, Escapable[]... escapableGroups) {
    return new TextEscaper(isEscapable, isEscapeCharacter, escapableGroups);
  }

  private final CharacterPredicate isEscapable;
  private final CharacterPredicate isEscapeCharacter;
  private final ImmutableMap<Character, Escapable> escapables;
  private final ImmutableMap<Character, Escapable> escapablesBySuffix;

  protected TextEscaper(CharacterPredicate isEscapable, CharacterPredicate isEscapeCharacter,
      Escapable[]... escapableGroups) {
    this.isEscapable = isEscapable;
    this.isEscapeCharacter = isEscapeCharacter;
    List<Character> escapeCharacters = new ArrayList<>();
    for (Escapable[] escapables : escapableGroups) {
      for (Escapable escapable : escapables) {
        if (escapable.getCharacter() == escapable.getEscapeCharacter()) {
          if (escapeCharacters.contains(escapable.getCharacter())) {
            throw new IllegalArgumentException("Escape character '" + escapable.getCharacter()
                + "' is repeated");
          }
          if (!isEscapeCharacter.apply(escapable.getCharacter())) {
            throw new IllegalArgumentException("Escape character '" + escapable.getCharacter()
                + "' does not apply to isEscapeCharacter predicate");
          }
          escapeCharacters.add(escapable.getCharacter());
        }
      }
    }

    List<Character> escapableCharacters = new ArrayList<>();
    ImmutableMap.Builder<Character, Escapable> escapablesBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<Character, Escapable> escapablesBySuffixBuilder = ImmutableMap.builder();
    for (Escapable[] escapables : escapableGroups) {
      for (Escapable escapable : escapables) {
        if (!escapeCharacters.contains(escapable.getEscapeCharacter())) {
          throw new IllegalArgumentException("Undefined escape character '" + escapable
              .getCharacter() + "'");
        }
        if (escapableCharacters.contains(escapable.getEscapeCharacter())) {
          throw new IllegalArgumentException("Escapable character '" + escapable.getCharacter()
              + "' is repeated");
        }
        if (!isEscapable.apply(escapable.getCharacter())) {
          throw new IllegalArgumentException("Escape character '" + escapable.getCharacter()
              + "' does not apply to isEscapable predicate");
        }
        escapablesBuilder.put(escapable.getCharacter(), escapable);
        escapablesBySuffixBuilder.put(escapable.getSuffixCharacter(), escapable);
        escapableCharacters.add(escapable.getCharacter());
      }
    }
    this.escapables = escapablesBuilder.build();
    this.escapablesBySuffix = escapablesBySuffixBuilder.build();
  }

  public String escape(String text) {
    Preconditions.checkArgument(text != null, "Can not escape null");

    String escapedText = text;
    final int textLength = text.length();

    for (int index = 0; index < textLength; index++) {
      char textCharacter = text.charAt(index);
      if (isEscapable(textCharacter)) {
        StringBuilder escapedTextBuilder = new StringBuilder(text.substring(0, index));

        for (; index < textLength; index++) {
          textCharacter = text.charAt(index);
          if (isEscapable(textCharacter)) {
            Escapable escapable = escapeOf(textCharacter);
            escapedTextBuilder.append(escapable.getEscapeCharacter());
            escapedTextBuilder.append(escapable.getSuffixCharacter());
          } else {
            escapedTextBuilder.append(textCharacter);
          }
        }

        escapedText = escapedTextBuilder.toString();
        break;
      }
    }

    return escapedText;
  }

  public void appendEscaped(StringBuilder stringBuilder, String text) {
    Preconditions.checkArgument(text != null, "Can not escape null");

    final int textLength = text.length();

    for (int index = 0; index < textLength; index++) {
      char textCharacter = text.charAt(index);
      if (isEscapable(textCharacter)) {
        for (; index < textLength; index++) {
          textCharacter = text.charAt(index);
          if (isEscapable(textCharacter)) {
            Escapable escapable = escapeOf(textCharacter);
            stringBuilder.append(escapable.getEscapeCharacter());
            stringBuilder.append(escapable.getSuffixCharacter());
          } else {
            stringBuilder.append(textCharacter);
          }
        }

        break;
      }
      stringBuilder.append(textCharacter);
    }
  }

  public String unescape(String escapedText) {
    Preconditions.checkArgument(escapedText != null, "Can not unescape null");

    String text = escapedText;
    final int escapedTextLength = escapedText.length();

    for (int index = 0; index < escapedTextLength; index++) {
      char textCharacter = text.charAt(index);
      if (isEscapeCharacter(textCharacter)) {
        StringBuilder textBuilder = new StringBuilder(text.substring(0, index));
        for (; index < escapedTextLength; index++) {
          textCharacter = text.charAt(index);
          if (isEscapeCharacter(textCharacter)) {
            index++;
            if (index >= escapedTextLength) {
              throw new IllegalArgumentException("Text '" + escapedText
                  + "' contains escape character and ended without providing a suffix");
            }
            Escapable escape = unescapeOfSuffix(text.charAt(index));
            textBuilder.append(escape.getCharacter());
          } else {
            textBuilder.append(textCharacter);
          }
        }
        text = textBuilder.toString();
        break;
      }
    }

    return text;
  }

  protected boolean isEscapeCharacter(char character) {
    return isEscapeCharacter.apply(character);
  }

  protected boolean isEscapable(char character) {
    return isEscapable.apply(character);
  }

  protected Escapable escapeOf(char character) {
    Escapable escapable = escapables.get(character);

    if (escapable == null) {
      throw new IllegalArgumentException("Character '" + character + "' is not escapable");
    }

    return escapable;
  }

  protected Escapable unescapeOfSuffix(char suffixCharacter) {
    Escapable escapable = escapablesBySuffix.get(suffixCharacter);

    if (escapable == null) {
      throw new IllegalArgumentException("Suffix escaped character '" + suffixCharacter
          + "' can not be escaped");
    }

    return escapable;
  }
}
