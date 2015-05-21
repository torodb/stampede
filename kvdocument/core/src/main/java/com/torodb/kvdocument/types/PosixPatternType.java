
package com.torodb.kvdocument.types;

/**
 *
 */
public class PosixPatternType implements DocType {
    private static final long serialVersionUID = 1L;

    public static final PosixPatternType INSTANCE = new PosixPatternType();
    
    private PosixPatternType()  {}
    
    @Override
    public <Result, Arg> Result accept(DocTypeVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.accept(this, arg);
    }

}
