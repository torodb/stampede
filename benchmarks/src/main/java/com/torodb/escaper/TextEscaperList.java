
/*
 * ToroDB - ToroDB-poc: Benchmarks
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
package com.torodb.escaper;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.torodb.common.util.TextEscaper;

public class TextEscaperList extends TextEscaper {
    
    public static TextEscaperList create(Escapable[]...escapableGroups) {
        return new TextEscaperList(escapableGroups);
    }
    
    private final ImmutableList<Escapable> escapes;
    private final ImmutableList<Escapable> escapables;
    
    private TextEscaperList(Escapable[]...escapableGroups) {
        super(new TextEscaper.CharacterPredicate() {
            @Override
            public boolean apply(char character) {
                return true;
            }
        },
        new TextEscaper.CharacterPredicate() {
            @Override
            public boolean apply(char character) {
                return true;
            }
        },
        escapableGroups);
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
                    throw new IllegalArgumentException("SqlEscapable character '" + escapable.getCharacter() + "' is repeated");
                }
                escapablesBuilder.add(escapable);
                escapableCharacters.add(escapable.getCharacter());
            }
        }
        this.escapables = escapablesBuilder.build();
    }
    
    @Override
    protected boolean isEscapeCharacter(char character) {
        for (Escapable escape : escapes) {
            if (escape.getCharacter() == character) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    protected boolean isEscapable(char character) {
        for (Escapable escape : escapables) {
            if (escape.getCharacter() == character) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    protected Escapable escapeOf(char character) {
        for (Escapable escape : escapables) {
            if (escape.getCharacter() == character) {
                return escape;
            }
        }
        throw new IllegalArgumentException("Character '" + character + "' is not escapable");
    }
    
    @Override
    protected Escapable unescapeOfSuffix(char suffixCharacter) {
        for (Escapable escapable : escapables) {
            if (escapable.getSuffixCharacter() == suffixCharacter) {
                return escapable;
            }
        }
        throw new IllegalArgumentException("Suffix escaped character '" + suffixCharacter + "' can not be escaped");
    }
}
