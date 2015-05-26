
package com.torodb.kvdocument.types;

/**
 *
 */
public class PatternType implements DocType {
    private static final long serialVersionUID = 1L;

    public static final PatternType INSTANCE = new PatternType();
    
    private PatternType()  {}
    
    @Override
    public <Result, Arg> Result accept(DocTypeVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.accept(this, arg);
    }

}
