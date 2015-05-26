/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.core.subdocument.values;

import com.torodb.torod.core.subdocument.BasicType;
import java.util.regex.Pattern;

/**
 *
 */
public class PatternValue implements Value<Pattern> {
    private static final long serialVersionUID = 1L;

    private final Pattern pattern;

    public PatternValue(Pattern pattern) {
        this.pattern = pattern;
    }
    
    @Override
    public Pattern getValue() {
        return pattern;
    }

    @Override
    public BasicType getType() {
        return BasicType.PATTERN;
    }

    @Override
    public String toString() {
        if (pattern.flags() == 0) {
            return pattern.pattern();
        }
        else {
            return toEmbeddedFlags() + '(' + pattern.pattern() + ')';
        }
    }
    
    private String toEmbeddedFlags() {
        int flags = pattern.flags();
        if (flags == 0) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder(30);
        sb.append("(?");
        if ((flags & Pattern.CANON_EQ) != 0) {
            throw new IllegalStateException("The flag CANON_EQ cannot be serialized as string");
        }
        if ((flags & Pattern.LITERAL) != 0) {
            throw new IllegalStateException("The flag LITERAL cannot be serialized as string");
        }
        if ((flags & Pattern.CASE_INSENSITIVE) != 0) {
            sb.append('i');
        }
        if ((flags & Pattern.COMMENTS) != 0) {
            sb.append('x');
        }
        if ((flags & Pattern.MULTILINE) != 0) {
            sb.append('m');
        }
        if ((flags & Pattern.DOTALL) != 0) {
            sb.append('s');
        }
        if ((flags & Pattern.UNICODE_CASE) != 0) {
            sb.append('u');
        }
        if ((flags & Pattern.UNIX_LINES) != 0) {
            sb.append('d');
        }
        sb.append(')');
        
        return sb.toString();
    }

    @Override
    public <Result, Arg> Result accept(ValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + (this.pattern != null ? this.pattern.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PatternValue other = (PatternValue) obj;
        return !((this.pattern == null) ? (other.pattern != null)
                : !this.pattern.equals(other.pattern));
    }

}
