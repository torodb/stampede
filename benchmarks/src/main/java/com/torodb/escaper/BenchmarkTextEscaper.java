
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

import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.torodb.common.util.TextEscaper;
import com.torodb.common.util.TextEscaper.CharacterPredicate;
import com.torodb.common.util.TextEscaper.Escapable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(
        value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
        justification = "State lifecycle is managed by JMH"
)
public class BenchmarkTextEscaper {

    private static final TextEscaper ESCAPER_LIST = TextEscaperList.create(InsertEscapable.values(), CopyEscapable.values());
    private static final TextEscaper ESCAPER_MAP = TextEscaperMap.create(InsertEscapable.values(), CopyEscapable.values());
    private static final TextEscaper ESCAPER_PREDICATE_SWITCH = TextEscaper.create(
            new CharacterPredicate() {
                public boolean apply(char c) {
                    switch(c) {
                    case ZERO_CHARACTER:
                    case ESCAPE_CHARACTER:
                    case COPY_ESCAPE_CHARACTER:
                    case ROW_DELIMETER_CHARACTER:
                    case COLUMN_DELIMETER_CHARACTER:
                    case CARRIAGE_RETURN_CHARACTER:
                        return true;
                    }
                    return false;
                }
            },
            new CharacterPredicate() {
                public boolean apply(char c) {
                    switch(c) {
                    case ESCAPE_CHARACTER:
                    case COPY_ESCAPE_CHARACTER:
                        return true;
                    }
                    return false;
                }
            },
            InsertEscapable.values(), CopyEscapable.values());
    private static final TextEscaper ESCAPER_PREDICATE_IFS = TextEscaper.create(
            new CharacterPredicate() {
                public boolean apply(char c) {
                    if (c == ESCAPE_CHARACTER) return true;
                    else if (c == ZERO_CHARACTER) return true;
                    else if (c == COPY_ESCAPE_CHARACTER) return true;
                    else if (c == ROW_DELIMETER_CHARACTER) return true;
                    else if (c == COLUMN_DELIMETER_CHARACTER) return true;
                    else if (c == CARRIAGE_RETURN_CHARACTER) return true;
                    return false;
                }
            },
            new CharacterPredicate() {
                public boolean apply(char c) {
                    if (c == ESCAPE_CHARACTER) return true;
                    else if (c == COPY_ESCAPE_CHARACTER) return true;
                    return false;
                }
            },
            InsertEscapable.values(), CopyEscapable.values());
    private static final TextEscaper ESCAPER_PREDICATE_BOOLEAN = TextEscaper.create(
            new CharacterPredicate() {
                public boolean apply(char c) {
                    return c == ESCAPE_CHARACTER ||
                            c == ZERO_CHARACTER ||
                            c == COPY_ESCAPE_CHARACTER ||
                            c == ROW_DELIMETER_CHARACTER ||
                            c == COLUMN_DELIMETER_CHARACTER ||
                            c == CARRIAGE_RETURN_CHARACTER;
                }
            },
            new CharacterPredicate() {
                public boolean apply(char c) {
                    return c == ESCAPE_CHARACTER ||
                            c == COPY_ESCAPE_CHARACTER;
                }
            },
            InsertEscapable.values(), CopyEscapable.values());
	
	@State(Scope.Thread)
	public static class TranslateState {
	    
		public String text;
		public String textWithEscapables;

		@Setup(Level.Iteration)
		public void setup(){
		    Random random = new Random();
            StringBuilder textBuilder = new StringBuilder();
            StringBuilder textWithEscapablesBuilder = new StringBuilder();
		    
		    char[] escapables = new char[] { 
	            ESCAPE_CHARACTER,
                ZERO_CHARACTER,
                COPY_ESCAPE_CHARACTER,
                ROW_DELIMETER_CHARACTER,
                COLUMN_DELIMETER_CHARACTER,
                CARRIAGE_RETURN_CHARACTER
		    };
		    
			for (int i = 0; i<10000; i++) {
                textBuilder.append('a');
                textWithEscapablesBuilder.append(i%1000==0?'a':escapables[i%escapables.length]);
			}
			
            text = textBuilder.toString();
            textWithEscapables = textWithEscapablesBuilder.toString();
		}
	}

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkList(TranslateState state, Blackhole blackhole) {
        ESCAPER_LIST.escape(state.text);
    }

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkMap(TranslateState state, Blackhole blackhole) {
        ESCAPER_MAP.escape(state.text);
    }

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkPredicateSwitch(TranslateState state, Blackhole blackhole) {
        ESCAPER_PREDICATE_SWITCH.escape(state.text);
    }

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkPredicateIfs(TranslateState state, Blackhole blackhole) {
        ESCAPER_PREDICATE_IFS.escape(state.text);
    }

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkPredicateBoolean(TranslateState state, Blackhole blackhole) {
        ESCAPER_PREDICATE_BOOLEAN.escape(state.text);
    }

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkWithEscapablesList(TranslateState state, Blackhole blackhole) {
        ESCAPER_LIST.escape(state.textWithEscapables);
    }

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkWithEscapablesMap(TranslateState state, Blackhole blackhole) {
        ESCAPER_MAP.escape(state.textWithEscapables);
    }

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkWithEscapablesPredicateSwitch(TranslateState state, Blackhole blackhole) {
        ESCAPER_PREDICATE_SWITCH.escape(state.textWithEscapables);
    }

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkWithEscapablesPredicateIfs(TranslateState state, Blackhole blackhole) {
        ESCAPER_PREDICATE_IFS.escape(state.textWithEscapables);
    }

    @Benchmark
    @Fork(value=5)
    @BenchmarkMode(value=Mode.Throughput)
    @Warmup(iterations=3)
    @Measurement(iterations=10) 
    public void benchmarkWithEscapablesPredicateBoolean(TranslateState state, Blackhole blackhole) {
        ESCAPER_PREDICATE_BOOLEAN.escape(state.textWithEscapables);
    }
    
    private final static char ESCAPE_CHARACTER = 1;
    private final static char ZERO_CHARACTER = 0;
    
    public enum InsertEscapable implements Escapable {
        ZERO(ZERO_CHARACTER, '0'),
        ESCAPE(ESCAPE_CHARACTER, '1');
        
        private final char character;
        private final char suffixCharacter;
        
        private InsertEscapable(char character, char suffixCharacter) {
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
