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

package com.torodb.torod.core;

/**
 *
 */
public enum WriteFailMode {

    /**
     * Given a list of write operations, if one of them fails, the previous are NOT commited and the following are NOT
     * executed.
     */
    TRANSACTIONAL,
    /**
     * Given a list of write operations, if one of them fails, the previous are commited and the following are
     * executed.
     */
    ISOLATED,
    /**
     * Given a list of write operations, if one of them fails, the previous are commited and the following are NOT
     * executed.
     */
    ORDERED;

}
