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

package com.torodb.torod.core.language.update;

import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.update.utils.UpdateActionVisitor;
import com.torodb.kvdocument.values.NumericDocValue;
import java.util.Collection;

/**
 *
 */
public class IncrementUpdateAction extends SingleFieldUpdateAction {

    private final NumericDocValue delta;

    public IncrementUpdateAction(
            Collection<AttributeReference> modifiedField,
            NumericDocValue delta) {
        
        super(modifiedField);
        this.delta = delta;
    }
    
    public NumericDocValue getDelta() {
        return delta;
    }

    @Override
    public <Result, Arg> Result accept(UpdateActionVisitor<Result, Arg> visitor, Arg argument) {
        return visitor.visit(this, argument);
    }
    
}
