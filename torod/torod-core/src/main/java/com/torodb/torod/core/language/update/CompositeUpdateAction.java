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

import com.google.common.collect.Maps;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.update.utils.UpdateActionVisitor;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class CompositeUpdateAction implements UpdateAction{
    
    private final Map<AttributeReference, UpdateAction> actions;

    CompositeUpdateAction(Map<AttributeReference, UpdateAction> actions) {
        this.actions = Collections.unmodifiableMap(actions);
    }

    public Map<AttributeReference, UpdateAction> getActions() {
        return actions;
    }

    @Override
    public <Result, Arg> Result accept(UpdateActionVisitor<Result, Arg> visitor, Arg argument) {
        return visitor.visit(this, argument);
    }
    
    public static class Builder {
        private final Map<AttributeReference, UpdateAction> actions = Maps.newHashMap();
        
        public Builder add(SingleFieldUpdateAction action) {
            for (AttributeReference modifiedField : action.getModifiedField()) {
                UpdateAction old = actions.put(modifiedField, action);
                if (old != null) {
                    throw new IllegalArgumentException(
                            "There are at least two update actions that "
                            + "modifies "+modifiedField+": "+old+" and "+action);
                }
            }
            return this;
        }
        
        public CompositeUpdateAction build() {
            return new CompositeUpdateAction(actions);
        }
    }
}
