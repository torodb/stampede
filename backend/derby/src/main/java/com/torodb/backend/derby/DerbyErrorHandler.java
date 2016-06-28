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

package com.torodb.backend.derby;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.torodb.backend.AbstractErrorHandlerInterface;

/**
 *
 */
@Singleton
public class DerbyErrorHandler extends AbstractErrorHandlerInterface {
    
    @Inject
    public DerbyErrorHandler() {
        super(
                "40001", 
                "40P01",
                "X0Y68", // Schema '?' already exists.
                "X0Y32"  // Table/View '?' already exists in Schema '?'.
                         // Column '?' already exists in Table/View '"?"."?"'.
                );
    }
}
