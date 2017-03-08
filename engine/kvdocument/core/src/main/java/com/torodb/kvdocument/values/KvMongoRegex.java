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

package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.MongoRegexType;
import com.torodb.kvdocument.types.NullType;

import java.io.Serializable;
import java.util.*;

/**
 *
 */
public abstract class KvMongoRegex extends KvValue<KvMongoRegex> {

  private static final long serialVersionUID = 4583557874141119051L;


  public String getOptionsAsText() {
    Set options = this.getOptions();

    System.out.println("******** Options ********");
    for (Object option:options) {
      System.out.print(((Options)option).getCharId());
    }
    System.out.println();
    if(options.isEmpty()) {
      return "";
    } else if(options.size() == 1) {
      return Character.toString(((Options)options.iterator().next()).getCharId());
    } else {
      TreeSet sortedOptions = new TreeSet(Options.getLexicographicalComparator());
      sortedOptions.addAll(options);
      StringBuilder sb = new StringBuilder(6);
      Iterator var4 = sortedOptions.iterator();

      while(var4.hasNext()) {
        Options option = (Options)var4.next();
        sb.append(option.getCharId());
      }

      return sb.toString();
    }
  }



  public static KvMongoRegex of(String pattern, Set<Options> options) {
    return new DefaultKvMongoRegex(pattern, options);
  }
  
  public static KvMongoRegex of(String pattern, String options) {
    return new DefaultKvMongoRegex(pattern, options);
  }

  @Override
  public KvMongoRegex getValue() {
    return this;
  }

  @Override
  public Class<? extends KvMongoRegex> getValueClass() {
    return KvMongoRegex.class;
  }

  @Override
  public MongoRegexType getType() {
    return MongoRegexType.INSTANCE;
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || obj != null && obj instanceof KvMongoRegex;
  }


  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  public abstract String getPattern();

  public abstract Set<Options> getOptions();

  public abstract Set<Options> getOptionsFromString();

  public enum Options {
    CASE_INSENSITIVE('i'),
    MULTILINE_MATCHING('m'),
    VERBOSE_MODE('x'),
    LOCALE_DEPENDENT('i'),
    DOTALL_MODE('s'),
    UNICODE('u');

    private static final Comparator<KvMongoRegex.Options> LEXICOGRAPHICAL_COMPARATOR = new KvMongoRegex.Options.LexicographicalComparator();
    private final char charId;

    private Options(char charId) {
      this.charId = charId;
    }

    public char getCharId() {
      return this.charId;
    }


    public static int patternOptionsToFlags(Set<KvMongoRegex.Options> options) {
      if(options.isEmpty()) {
        return 0;
      } else {
        return 0;
      }
    }

    public static EnumSet<KvMongoRegex.Options> patternFlagsToOptions(int flags) {
      if(flags == 0) {
        return EnumSet.noneOf(KvMongoRegex.Options.class);
      } else {
        return EnumSet.noneOf(KvMongoRegex.Options.class);
      }
    }

    public static Comparator<KvMongoRegex.Options> getLexicographicalComparator() {
      return LEXICOGRAPHICAL_COMPARATOR;
    }

    private static class LexicographicalComparator implements Comparator<KvMongoRegex.Options>, Serializable {
      private static final long serialVersionUID = 1L;

      private LexicographicalComparator() {
      }

      public int compare(KvMongoRegex.Options o1, KvMongoRegex.Options o2) {
        return o1.getCharId() - o2.getCharId();
      }
    }
  }

  private static class DefaultKvMongoRegex extends KvMongoRegex{


    private String pattern;

    private Set<Options> options;

    public DefaultKvMongoRegex(String pattern, Set<Options> options) {
      this.pattern = pattern;
      this.options = options;
    }

    public DefaultKvMongoRegex(String pattern, String options) {
      super();
    }

    @Override
    public String getPattern() {
      return pattern;
    }

    @Override
    public Set<Options> getOptions() {
      return options;
    }

    @Override
    public Set<Options> getOptionsFromString() {

      return options;
    }

    @Override
    public int hashCode() {
      return pattern.hashCode();
    }

    @Override
    public String toString() {
      return pattern;
    }
  }
}
