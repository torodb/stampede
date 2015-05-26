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

package com.torodb.torod.core.language.querycriteria.utils;

import com.torodb.torod.core.language.querycriteria.*;

/**
 *
 * @param <Result>
 * @param <Arg>
 */
public interface QueryCriteriaVisitor<Result, Arg> {

    Result visit(TrueQueryCriteria criteria, Arg arg);
    
    Result visit(FalseQueryCriteria criteria, Arg arg);
    
    Result visit(AndQueryCriteria criteria, Arg arg);

    Result visit(OrQueryCriteria criteria, Arg arg);

    Result visit(NotQueryCriteria criteria, Arg arg);

    Result visit(TypeIsQueryCriteria criteria, Arg arg);

    Result visit(IsEqualQueryCriteria criteria, Arg arg);

    Result visit(IsGreaterQueryCriteria criteria, Arg arg);

    Result visit(IsGreaterOrEqualQueryCriteria criteria, Arg arg);

    Result visit(IsLessQueryCriteria criteria, Arg arg);

    Result visit(IsLessOrEqualQueryCriteria criteria, Arg arg);
    
    Result visit(IsObjectQueryCriteria criteria, Arg arg);
    
    Result visit(InQueryCriteria criteria, Arg arg);
    
    Result visit(ModIsQueryCriteria criteria, Arg arg);
    
    Result visit(SizeIsQueryCriteria criteria, Arg arg);
    
    Result visit(ContainsAttributesQueryCriteria criteria, Arg arg);

    Result visit(ExistsQueryCriteria criteria, Arg arg);
    
    Result visit(MatchPatternQueryCriteria criteria, Arg arg);
}
