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
package com.torodb.torod.core.subdocument.values.heap;

import com.torodb.torod.core.subdocument.values.ScalarDate;
import org.threeten.bp.LocalDate;

/**
 *
 */
public class LocalDateScalarDate extends ScalarDate {

    private static final long serialVersionUID = 2710590023434476699L;

    private final LocalDate value;

    public LocalDateScalarDate(LocalDate localDate) {
        this.value = localDate;
    }

    @Override
    public LocalDate getValue() {
        return value;
    }

}
