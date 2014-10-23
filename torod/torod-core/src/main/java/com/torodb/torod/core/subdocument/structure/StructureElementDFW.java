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


package com.torodb.torod.core.subdocument.structure;

/**
 *
 */
public class StructureElementDFW<Arg> implements StructureElementVisitor<Void, Arg> {

    protected void preDocStructure(DocStructure docStructure, Arg arg) {
    }

    protected void postDocStructure(DocStructure docStructure, Arg arg) {
    }

    protected void preArrayStrcture(ArrayStructure array, Arg arg) {
    }

    protected void postArrayStrcture(ArrayStructure array, Arg arg) {
    }

    @Override
    public Void visit(DocStructure structure, Arg arg) {
        preDocStructure(structure, arg);
        for (StructureElement element : structure.getElements().values()) {
                element.accept(this, arg);
            }
        postDocStructure(structure, arg);
        
        return null;
    }

    @Override
    public Void visit(ArrayStructure array, Arg arg) {
        preArrayStrcture(array, arg);
        for (StructureElement element : array.getElements().values()) {
                element.accept(this, arg);
            }
        postArrayStrcture(array, arg);
        
        return null;
    }
    
}
