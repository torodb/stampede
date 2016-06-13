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

package com.torodb.core.d2r;

import com.torodb.core.transaction.metainf.MetaDocPart;

public class DocPartResult<Result> {
    private final MetaDocPart metaDocPart;
    private final Result result;
    
    public DocPartResult(MetaDocPart metaDocPart, Result result) {
        super();
        this.metaDocPart = metaDocPart;
        this.result = result;
    }
    
    public MetaDocPart getMetaDocPart() {
        return metaDocPart;
    }
    
    public Result getResult() {
        return result;
    }
}
