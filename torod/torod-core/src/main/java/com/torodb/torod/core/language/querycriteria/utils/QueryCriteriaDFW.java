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
 */
public class QueryCriteriaDFW<Arg> implements QueryCriteriaVisitor<Void, Arg>{

    @Override
    public Void visit(TrueQueryCriteria criteria, Arg arg) {
        return null;
    }

    @Override
    public Void visit(FalseQueryCriteria criteria, Arg arg) {
        return null;
    }
    
    protected void preAnd(AndQueryCriteria criteria, Arg arg) {
    }
    
    protected void postAnd(AndQueryCriteria criteria, Arg arg) {
    }

    @Override
    public Void visit(AndQueryCriteria criteria, Arg arg) {
        preAnd(criteria, arg);
        
        criteria.getSubQueryCriteria1().accept(this, arg);
        criteria.getSubQueryCriteria2().accept(this, arg);
        
        postAnd(criteria, arg);
        
        return null;
    }
    
    protected void preOr(OrQueryCriteria criteria, Arg arg) {
    }
    
    protected void postOr(OrQueryCriteria criteria, Arg arg) {
    }

    @Override
    public Void visit(OrQueryCriteria criteria, Arg arg) {
        preOr(criteria, arg);
        
        criteria.getSubQueryCriteria1().accept(this, arg);
        criteria.getSubQueryCriteria2().accept(this, arg);
        
        postOr(criteria, arg);
        
        return null;
    }

    
    protected void preNot(NotQueryCriteria criteria, Arg arg) {
    }
    
    protected void postNot(NotQueryCriteria criteria, Arg arg) {
    }

    @Override
    public Void visit(NotQueryCriteria criteria, Arg arg) {
        preNot(criteria, arg);
        
        criteria.getSubQueryCriteria().accept(this, arg);
        
        postNot(criteria, arg);
        
        return null;
    }
    
    protected void preAttributeQuery(AttributeQueryCriteria criteria, Arg arg) {
    }
    
    protected void postAttributeQuery(AttributeQueryCriteria criteria, Arg arg) {
    }

    protected void preTypeIs(TypeIsQueryCriteria criteria, Arg arg) {
    }
    
    protected void postTypeIs(TypeIsQueryCriteria criteria, Arg arg) {
    }
    
    @Override
    public Void visit(TypeIsQueryCriteria criteria, Arg arg) {
        preTypeIs(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postTypeIs(criteria, arg);
        
        return null;
    }
    
    protected void preAttributeAndValueQuery(AttributeAndValueQueryCriteria criteria, Arg arg) {
    }
    protected void postAttributeAndValueQuery(AttributeAndValueQueryCriteria criteria, Arg arg) {
    }

    protected void preIsEqual(IsEqualQueryCriteria criteria, Arg arg) {
    }
    protected void postIsEqual(IsEqualQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(IsEqualQueryCriteria criteria, Arg arg) {
        preIsEqual(criteria, arg);
        preAttributeAndValueQuery(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postAttributeAndValueQuery(criteria, arg);
        postIsEqual(criteria, arg);
        
        return null;
    }

    protected void preIsGreater(IsGreaterQueryCriteria criteria, Arg arg) {
    }
    protected void postIsGreater(IsGreaterQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(IsGreaterQueryCriteria criteria, Arg arg) {
        preIsGreater(criteria, arg);
        preAttributeAndValueQuery(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postAttributeAndValueQuery(criteria, arg);
        postIsGreater(criteria, arg);
        
        return null;
    }

    protected void preIsGreaterOrEqual(IsGreaterOrEqualQueryCriteria criteria, Arg arg) {
    }
    protected void postIsGreaterOrEqual(IsGreaterOrEqualQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(IsGreaterOrEqualQueryCriteria criteria, Arg arg) {
        preIsGreaterOrEqual(criteria, arg);
        preAttributeAndValueQuery(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postAttributeAndValueQuery(criteria, arg);
        postIsGreaterOrEqual(criteria, arg);
        
        return null;
    }

    protected void preIsLess(IsLessQueryCriteria criteria, Arg arg) {
    }
    protected void postIsLess(IsLessQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(IsLessQueryCriteria criteria, Arg arg) {
        preIsLess(criteria, arg);
        preAttributeAndValueQuery(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postAttributeAndValueQuery(criteria, arg);
        postIsLess(criteria, arg);
        
        return null;
    }

    protected void preIsLessOrEqual(IsLessOrEqualQueryCriteria criteria, Arg arg) {
    }
    protected void postIsLessOrEqual(IsLessOrEqualQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(IsLessOrEqualQueryCriteria criteria, Arg arg) {
        preIsLessOrEqual(criteria, arg);
        preAttributeAndValueQuery(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postAttributeAndValueQuery(criteria, arg);
        postIsLessOrEqual(criteria, arg);
        
        return null;
    }

    protected void preIsObject(IsObjectQueryCriteria criteria, Arg arg) {
    }
    protected void postIsObject(IsObjectQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(IsObjectQueryCriteria criteria, Arg arg) {
        preIsObject(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postIsObject(criteria, arg);
        
        return null;
    }

    protected void preIn(InQueryCriteria criteria, Arg arg) {
    }
    protected void postIn(InQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(InQueryCriteria criteria, Arg arg) {
        preIn(criteria, arg);
        preAttributeAndValueQuery(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postAttributeAndValueQuery(criteria, arg);
        postIn(criteria, arg);
        
        return null;
    }


    protected void preModIs(ModIsQueryCriteria criteria, Arg arg) {
    }
    protected void postModIs(ModIsQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(ModIsQueryCriteria criteria, Arg arg) {
        preModIs(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postModIs(criteria, arg);
        
        return null;
    }
    
    

    protected void preSizeIs(SizeIsQueryCriteria criteria, Arg arg) {
    }
    protected void postSizeIs(SizeIsQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(SizeIsQueryCriteria criteria, Arg arg) {
        preSizeIs(criteria, arg);
        preAttributeAndValueQuery(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postAttributeAndValueQuery(criteria, arg);
        postSizeIs(criteria, arg);
        
        return null;
    }

    

    protected void preContainsAttributes(ContainsAttributesQueryCriteria criteria, Arg arg) {
    }
    protected void postContainsAttributes(ContainsAttributesQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(ContainsAttributesQueryCriteria criteria, Arg arg) {
        preContainsAttributes(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postContainsAttributes(criteria, arg);
        
        return null;
    }

    protected void preExists(ExistsQueryCriteria criteria, Arg arg) {
    }
    protected void postExists(ExistsQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(ExistsQueryCriteria criteria, Arg arg) {
        preExists(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        criteria.getBody().accept(this, arg);
        
        postAttributeQuery(criteria, arg);
        postExists(criteria, arg);
        
        return null;
    }

    protected void preMatchPattern(MatchPatternQueryCriteria criteria, Arg arg) {
    }
    protected void postMatchPattern(MatchPatternQueryCriteria criteria, Arg arg) {
    }
    @Override
    public Void visit(MatchPatternQueryCriteria criteria, Arg arg) {
        preMatchPattern(criteria, arg);
        preAttributeQuery(criteria, arg);
        
        postAttributeQuery(criteria, arg);
        postMatchPattern(criteria, arg);
        
        return null;
    }
    
}
