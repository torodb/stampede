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
package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.DocType;
import com.torodb.kvdocument.types.PosixPatternType;
import java.util.regex.Pattern;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a POSIX regex.
 */
@Immutable
public class PosixPatternValue implements DocValue {
    private static final Logger LOGGER
            = LoggerFactory.getLogger(PosixPatternValue.class);
    private final String posixPattern;

    private PosixPatternValue(String posixPattern) {
        this.posixPattern = posixPattern;
    }

    @Override
    public String getValue() {
        return posixPattern;
    }

    @Override
    public DocType getType() {
        return PosixPatternType.INSTANCE;
    }

    @Override
    public <Result, Arg> Result accept(DocValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
    
    public static PosixPatternValue fromPosixPattern(String posixPattern) {
        return new PosixPatternValue(posixPattern);
    }

    public Pattern toJavaPattern() {
        LOGGER.warn("Translation from POSIX pattern to Java pattern is not "
                + "completely implemented");
        return Pattern.compile(posixPattern);
    }
    
    public static PosixPatternValue fromJavaPattern(Pattern pattern) {
        LOGGER.warn("Translation from Java pattern to POSIX pattern is not "
                + "completely implemented");
        return new PosixPatternValue(pattern.pattern());
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 29 * hash + (this.posixPattern != null ?
                this.posixPattern.hashCode() : 0);
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
        final PosixPatternValue other = (PosixPatternValue) obj;
        if ((this.posixPattern == null) ? (other.posixPattern != null)
                : !this.posixPattern.equals(other.posixPattern)) {
            return false;
        }
        return true;
    }

    
}
