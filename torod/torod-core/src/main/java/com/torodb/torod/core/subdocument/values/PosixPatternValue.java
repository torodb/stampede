
package com.torodb.torod.core.subdocument.values;

import com.torodb.torod.core.subdocument.BasicType;

/**
 *
 */
public class PosixPatternValue implements Value<String> {
    private static final long serialVersionUID = 1L;

    private final String pattern;

    public PosixPatternValue(String pattern) {
        this.pattern = pattern;
    }
    
    @Override
    public String getValue() {
        return pattern;
    }

    @Override
    public BasicType getType() {
        return BasicType.POSIX_PATTERN;
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
        final PosixPatternValue other = (PosixPatternValue) obj;
        if ((this.pattern == null) ? (other.pattern != null)
                : !this.pattern.equals(other.pattern)) {
            return false;
        }
        return true;
    }

}
