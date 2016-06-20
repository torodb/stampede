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

package com.torodb.d2r;

import com.torodb.core.backend.IdentifierInterface;

public class MockIdentifierInterface implements IdentifierInterface {

    @Override
    public int identifierMaxSize() {
        return 128;
    }

    @Override
    public boolean isAllowedSchemaIdentifier(String identifier) {
        return !identifier.equals("unallowed_schema");
    }

    @Override
    public boolean isAllowedTableIdentifier(String identifier) {
        return !identifier.equals("unallowed_table");
    }

    @Override
    public boolean isAllowedColumnIdentifier(String identifier) {
        return !identifier.equals("unallowed_column_s");
    }

    @Override
    public boolean isSameIdentifier(String leftIdentifier, String rightIdentifier) {
        return leftIdentifier.equals(rightIdentifier);
    }

}
