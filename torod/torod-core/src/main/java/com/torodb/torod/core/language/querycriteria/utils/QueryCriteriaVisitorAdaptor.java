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
public class QueryCriteriaVisitorAdaptor<Result, Arg> implements QueryCriteriaVisitor<Result, Arg> {

    protected Result defaultCase(QueryCriteria criteria, Arg arg) {
        return null;
    }
    
    protected Result visit(AttributeQueryCriteria criteria, Arg arg) {
        return defaultCase(criteria, arg);
    }
    
    protected Result visit(AttributeAndValueQueryCriteria criteria, Arg arg) {
        return visit((AttributeQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(TrueQueryCriteria criteria, Arg arg) {
        return defaultCase(criteria, arg);
    }

    @Override
    public Result visit(FalseQueryCriteria criteria, Arg arg) {
        return defaultCase(criteria, arg);
    }

    @Override
    public Result visit(AndQueryCriteria criteria, Arg arg) {
        return defaultCase(criteria, arg);
    }

    @Override
    public Result visit(OrQueryCriteria criteria, Arg arg) {
        return defaultCase(criteria, arg);
    }

    @Override
    public Result visit(NotQueryCriteria criteria, Arg arg) {
        return defaultCase(criteria, arg);
    }

    @Override
    public Result visit(TypeIsQueryCriteria criteria, Arg arg) {
        return visit((AttributeQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(IsEqualQueryCriteria criteria, Arg arg) {
        return visit((AttributeAndValueQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(IsGreaterQueryCriteria criteria, Arg arg) {
        return visit((AttributeAndValueQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(IsGreaterOrEqualQueryCriteria criteria, Arg arg) {
        return visit((AttributeAndValueQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(IsLessQueryCriteria criteria, Arg arg) {
        return visit((AttributeAndValueQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(IsLessOrEqualQueryCriteria criteria, Arg arg) {
        return visit((AttributeAndValueQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(IsObjectQueryCriteria criteria, Arg arg) {
        return visit((AttributeQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(InQueryCriteria criteria, Arg arg) {
        return visit((AttributeQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(ModIsQueryCriteria criteria, Arg arg) {
        return visit((AttributeQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(SizeIsQueryCriteria criteria, Arg arg) {
        return visit((AttributeAndValueQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(ContainsAttributesQueryCriteria criteria, Arg arg) {
        return visit((AttributeQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(ExistsQueryCriteria criteria, Arg arg) {
        return visit((AttributeQueryCriteria) criteria, arg);
    }

    @Override
    public Result visit(MatchPatternQueryCriteria criteria, Arg arg) {
        return visit((AttributeQueryCriteria) criteria, arg);
    }

}
