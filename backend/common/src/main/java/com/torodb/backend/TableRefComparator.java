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

package com.torodb.backend;

import java.sql.ResultSet;
import java.util.Comparator;

import com.torodb.core.d2r.DocPartResult;

public class TableRefComparator {
    public static class MetaDocPart implements Comparator<com.torodb.core.transaction.metainf.MetaDocPart> {
        public static final MetaDocPart ASC = new MetaDocPart();
        public static final DescMetaDocPart DESC = new DescMetaDocPart();
        
        private MetaDocPart() {
        }
        
        @Override
        public int compare(com.torodb.core.transaction.metainf.MetaDocPart leftMetaDocPart, 
                com.torodb.core.transaction.metainf.MetaDocPart rightMetaDocPart) {
            return leftMetaDocPart.getTableRef().getDepth() -
                    rightMetaDocPart.getTableRef().getDepth();
        }
    }
    private static class DescMetaDocPart implements Comparator<com.torodb.core.transaction.metainf.MetaDocPart> {
        private DescMetaDocPart() {
        }
        
        @Override
        public int compare(com.torodb.core.transaction.metainf.MetaDocPart leftMetaDocPart, 
                com.torodb.core.transaction.metainf.MetaDocPart rightMetaDocPart) {
            return rightMetaDocPart.getTableRef().getDepth() -
                    leftMetaDocPart.getTableRef().getDepth();
        }
    }
    public static class MutableMetaDocPart implements Comparator<com.torodb.core.transaction.metainf.MetaDocPart> {
        public static final MutableMetaDocPart ASC = new MutableMetaDocPart();
        public static final DescMutableMetaDocPart DESC = new DescMutableMetaDocPart();
        
        private MutableMetaDocPart() {
        }
        
        @Override
        public int compare(com.torodb.core.transaction.metainf.MetaDocPart leftMetaDocPart, 
                com.torodb.core.transaction.metainf.MetaDocPart rightMetaDocPart) {
            return leftMetaDocPart.getTableRef().getDepth() -
                    rightMetaDocPart.getTableRef().getDepth();
        }
    }
    private static class DescMutableMetaDocPart implements Comparator<com.torodb.core.transaction.metainf.MetaDocPart> {
        private DescMutableMetaDocPart() {
        }
        
        @Override
        public int compare(com.torodb.core.transaction.metainf.MetaDocPart leftMetaDocPart, 
                com.torodb.core.transaction.metainf.MetaDocPart rightMetaDocPart) {
            return rightMetaDocPart.getTableRef().getDepth() -
                    leftMetaDocPart.getTableRef().getDepth();
        }
    }
    
    public static class DocPartResultSet implements Comparator<DocPartResult<ResultSet>> {
        public static final DocPartResultSet DESC = new DocPartResultSet();
    
        private DocPartResultSet() {
        }
        
        @Override
        public int compare(DocPartResult<ResultSet> leftDocPartResultSet, 
                DocPartResult<ResultSet> rightDocPartResultSet) {
            return rightDocPartResultSet.getMetaDocPart().getTableRef().getDepth() -
                    leftDocPartResultSet.getMetaDocPart().getTableRef().getDepth();
        }
    }
}
