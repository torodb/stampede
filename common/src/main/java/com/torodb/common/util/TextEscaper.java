/*
 * MongoWP - ToroDB-poc: common
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.common.util;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class TextEscaper {
    
    public static TextEscaper create(Escapable[]...escapableGroups) {
        return new TextEscaper(escapableGroups);
    }
    
    private final ImmutableList<Escapable> escapes;
    private final ImmutableList<Escapable> escapables;
    
    private TextEscaper(Escapable[]...escapableGroups) {
        List<Character> escapeCharacters = new ArrayList<>();
        ImmutableList.Builder<Escapable> escapesBuilder = ImmutableList.builder();
        for (Escapable[] escapables : escapableGroups) {
            for (Escapable escapable : escapables) {
                if (escapable.getCharacter() == escapable.getEscapeCharacter()) {
                    if (escapeCharacters.contains(escapable.getCharacter())) {
                        throw new IllegalArgumentException("Escape character '" + escapable.getCharacter() + "' is repeated");
                    }
                    escapesBuilder.add(escapable);
                    escapeCharacters.add(escapable.getCharacter());
                }
            }
        }
        this.escapes = escapesBuilder.build();
        
        List<Character> escapableCharacters = new ArrayList<>();
        ImmutableList.Builder<Escapable> escapablesBuilder = ImmutableList.builder();
        for (Escapable[] escapables : escapableGroups) {
            for (Escapable escapable : escapables) {
                if (!escapeCharacters.contains(escapable.getEscapeCharacter())) {
                    throw new IllegalArgumentException("Undefined escape character '" + escapable.getCharacter() + "'");
                }
                if (escapableCharacters.contains(escapable.getEscapeCharacter())) {
                    throw new IllegalArgumentException("Escapable character '" + escapable.getCharacter() + "' is repeated");
                }
                escapablesBuilder.add(escapable);
                escapableCharacters.add(escapable.getCharacter());
            }
        }
        this.escapables = escapablesBuilder.build();
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
                            throw new IllegalArgumentException("Text '" + escapedText + "' contains escape character and ended without providing a suffix");
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
    
    private boolean isEscapeCharacter(char character) {
        for (Escapable escape : escapes) {
            if (escape.getCharacter() == character) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isEscapable(char character) {
        for (Escapable escape : escapables) {
            if (escape.getCharacter() == character) {
                return true;
            }
        }
        return false;
    }
    
    private Escapable escapeOf(char character) {
        for (Escapable escape : escapables) {
            if (escape.getCharacter() == character) {
                return escape;
            }
        }
        throw new IllegalArgumentException("Character '" + character + "' is not escapable");
    }
    
    private Escapable unescapeOfSuffix(char suffixCharacter) {
        for (Escapable escapable : escapables) {
            if (escapable.getSuffixCharacter() == suffixCharacter) {
                return escapable;
            }
        }
        throw new IllegalArgumentException("Suffix escaped character '" + suffixCharacter + "' can not be escaped");
    }
    
    public interface Escapable {
        public char getCharacter();
        public char getSuffixCharacter();
        public char getEscapeCharacter();
    }
}
