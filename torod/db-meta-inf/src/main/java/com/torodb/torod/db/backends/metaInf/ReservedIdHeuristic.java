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

package com.torodb.torod.db.backends.metaInf;

import javax.annotation.Nonnegative;

/**
 *
 */
public interface ReservedIdHeuristic {

    /**
     * Given the last used id and the last cached id, returns the number of ids that should be cached.
     * <p>
     * Last used id can be higher than last cached id.
     * <p>
     * The result of this method plus cachedId must be equal or higher than freeId
     * <p>
     * @param usedId the last used id
     * @param cachedId the last cached id
     * @return
     */
    @Nonnegative
    public int evaluate(int usedId, int cachedId);
}
