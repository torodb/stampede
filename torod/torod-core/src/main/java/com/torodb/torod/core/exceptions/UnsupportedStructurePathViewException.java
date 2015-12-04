
package com.torodb.torod.core.exceptions;

import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;

/**
 *
 */
public class UnsupportedStructurePathViewException extends ToroException {

    private static final long serialVersionUID = 1L;

    private final DocStructure unsupportedStructure;
    private final StructureElement unsupportedNode;

    public UnsupportedStructurePathViewException(DocStructure unsupportedStructure, StructureElement unsupportedNode) {
        this.unsupportedStructure = unsupportedStructure;
        this.unsupportedNode = unsupportedNode;
    }

    public UnsupportedStructurePathViewException(DocStructure unsupportedStructure, StructureElement unsupportedNode, String message) {
        super(message);
        this.unsupportedStructure = unsupportedStructure;
        this.unsupportedNode = unsupportedNode;
    }

    public UnsupportedStructurePathViewException(DocStructure unsupportedStructure, StructureElement unsupportedNode, String message, Throwable cause) {
        super(message, cause);
        this.unsupportedStructure = unsupportedStructure;
        this.unsupportedNode = unsupportedNode;
    }

    public DocStructure getUnsupportedStructure() {
        return unsupportedStructure;
    }

    public StructureElement getUnsupportedNode() {
        return unsupportedNode;
    }

}
