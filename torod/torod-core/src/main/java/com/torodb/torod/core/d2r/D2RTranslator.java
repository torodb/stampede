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

package com.torodb.torod.core.d2r;

import com.google.common.base.Function;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.ToroDocument;

/**
 *
 */
public interface D2RTranslator {

    public void initialize();

//    public SplitDocument translate(SessionExecutor sessionExecutor, String collection, ToroDocument document);
//
//    public ToroDocument translate(SplitDocument splitDocument);

    public Function<SplitDocument, ToroDocument> getToDocumentFunction();

    public Function<ToroDocument, SplitDocument> getToRelationalFunction(SessionExecutor sessionExecutor, String collection);
    
    public void shutdown();
    
    public void shutdownNow();
}
