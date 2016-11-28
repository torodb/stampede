/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend;

import com.torodb.core.d2r.DocPartResult;

import java.io.Serializable;
import java.util.Comparator;

public class TableRefComparator {

  public static class MetaDocPart implements
      Comparator<com.torodb.core.transaction.metainf.MetaDocPart>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final MetaDocPart ASC = new MetaDocPart();
    public static final DescMetaDocPart DESC = new DescMetaDocPart();

    private MetaDocPart() {
    }

    @Override
    public int compare(com.torodb.core.transaction.metainf.MetaDocPart leftMetaDocPart,
        com.torodb.core.transaction.metainf.MetaDocPart rightMetaDocPart) {
      return leftMetaDocPart.getTableRef().getDepth() - rightMetaDocPart.getTableRef().getDepth();
    }
  }

  private static class DescMetaDocPart implements
      Comparator<com.torodb.core.transaction.metainf.MetaDocPart>, Serializable {

    private static final long serialVersionUID = 1L;

    private DescMetaDocPart() {
    }

    @Override
    public int compare(com.torodb.core.transaction.metainf.MetaDocPart leftMetaDocPart,
        com.torodb.core.transaction.metainf.MetaDocPart rightMetaDocPart) {
      return rightMetaDocPart.getTableRef().getDepth() - leftMetaDocPart.getTableRef().getDepth();
    }
  }

  public static class MutableMetaDocPart implements
      Comparator<com.torodb.core.transaction.metainf.MetaDocPart>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final MutableMetaDocPart ASC = new MutableMetaDocPart();
    public static final DescMutableMetaDocPart DESC = new DescMutableMetaDocPart();

    private MutableMetaDocPart() {
    }

    @Override
    public int compare(com.torodb.core.transaction.metainf.MetaDocPart leftMetaDocPart,
        com.torodb.core.transaction.metainf.MetaDocPart rightMetaDocPart) {
      return leftMetaDocPart.getTableRef().getDepth() - rightMetaDocPart.getTableRef().getDepth();
    }
  }

  private static class DescMutableMetaDocPart implements
      Comparator<com.torodb.core.transaction.metainf.MetaDocPart>, Serializable {

    private static final long serialVersionUID = 1L;

    private DescMutableMetaDocPart() {
    }

    @Override
    public int compare(com.torodb.core.transaction.metainf.MetaDocPart leftMetaDocPart,
        com.torodb.core.transaction.metainf.MetaDocPart rightMetaDocPart) {
      return rightMetaDocPart.getTableRef().getDepth() - leftMetaDocPart.getTableRef().getDepth();
    }
  }

  public static class DocPartResultSet implements Comparator<DocPartResult>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final DocPartResultSet DESC = new DocPartResultSet();

    private DocPartResultSet() {
    }

    @Override
    public int compare(DocPartResult leftDocPartResultSet,
        DocPartResult rightDocPartResultSet) {
      return rightDocPartResultSet.getMetaDocPart().getTableRef().getDepth() - leftDocPartResultSet
          .getMetaDocPart().getTableRef().getDepth();
    }
  }
}
