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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** */
public abstract class KvMongoRegex extends KvValue<KvMongoRegex> {

  private static final long serialVersionUID = 4583557874141119051L;

  public abstract String getOptionsAsText();

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

  public enum Options {
    CASE_INSENSITIVE('i'),
    MULTILINE_MATCHING('m'),
    VERBOSE_MODE('x'),
    LOCALE_DEPENDENT('i'),
    DOTALL_MODE('s'),
    UNICODE('u');

    private static final Comparator<KvMongoRegex.Options> LEXICOGRAPHICAL_COMPARATOR =
        new KvMongoRegex.Options.LexicographicalComparator();
    private final char charId;

    private Options(char charId) {
      this.charId = charId;
    }

    public char getCharId() {
      return this.charId;
    }

    public static Comparator<KvMongoRegex.Options> getLexicographicalComparator() {
      return LEXICOGRAPHICAL_COMPARATOR;
    }

    private static class LexicographicalComparator
        implements Comparator<KvMongoRegex.Options>, Serializable {
      private static final long serialVersionUID = 1L;

      private LexicographicalComparator() {}

      public int compare(KvMongoRegex.Options o1, KvMongoRegex.Options o2) {
        return o1.getCharId() - o2.getCharId();
      }
    }
  }

  private static class DefaultKvMongoRegex extends KvMongoRegex {

    private static final long serialVersionUID = -324759198672148662L;
    private String pattern;

    private String options;

    private DefaultKvMongoRegex(String pattern, String options) {
      this.pattern = pattern;
      this.options = options;
    }

    @Override
    public String getOptionsAsText() {
      return options;
    }

    @Override
    public String getPattern() {
      return pattern;
    }

    @Override
    public Set<Options> getOptions() {
      return getOptionsFromString(options);
    }

    private static Set<Options> getOptionsFromString(String optString) {

      if (optString.isEmpty()) {
        return EnumSet.noneOf(Options.class);
      }

      final List<Options> optionList =
          optString
              .chars()
              .mapToObj(i -> (char) i)
              .map(
                  character ->
                      Arrays.stream(Options.values())
                          .filter(opt -> opt.charId == character)
                          .findAny()
                          .get())
              .collect(Collectors.toList());

      return EnumSet.copyOf(optionList);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      DefaultKvMongoRegex that = (DefaultKvMongoRegex) o;

      if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null) {
        return false;
      }
      return options != null ? options.equals(that.options) : that.options == null;
    }

    @Override
    public int hashCode() {
      int result = pattern != null ? pattern.hashCode() : 0;
      result = 31 * result + (options != null ? options.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return pattern + "/" + getOptionsAsText();
    }
  }
}
