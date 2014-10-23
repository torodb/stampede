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

package com.torodb.torod.core.language.update.utils;

import com.torodb.torod.core.language.update.*;

/**
 *
 */
public interface UpdateActionVisitor<Result, Argument> {

    Result visit(IncrementUpdateAction action, Argument arg);

    Result visit(MultiplyUpdateAction action, Argument arg);

    Result visit(MoveUpdateAction action, Argument arg);

    Result visit(SetCurrentDateUpdateAction action, Argument arg);

    Result visit(SetFieldUpdateAction action, Argument arg);
    
    Result visit(SetDocumentUpdateAction action, Argument arg);

    Result visit(UnsetFieldUpdateAction action, Argument arg);
    
    Result visit(CompositeUpdateAction action, Argument arg);

}
