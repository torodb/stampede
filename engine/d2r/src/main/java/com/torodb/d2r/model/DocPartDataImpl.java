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

package com.torodb.d2r.model;

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DocPartDataImpl implements DocPartData {

  private List<DocPartRow> docPartRows = new ArrayList<>();
  private TableMetadata metadata;
  private DocPartDataImpl parent;
  private List<DocPartDataImpl> childs = null;

  public DocPartDataImpl(TableMetadata metadata, DocPartDataImpl parent) {
    this.metadata = metadata;
    this.parent = parent;
    if (parent != null) {
      this.parent.addChild(this);
    }
  }

  public DocPartRowImpl newRowObject(Integer index, DocPartRowImpl parentRow) {
    DocPartRowImpl docPartRow = new DocPartRowImpl(metadata, index, parentRow, this);
    docPartRows.add(docPartRow);
    return docPartRow;
  }

  public MetaDocPart getMetaDocPart() {
    return metadata.getMetaDocPart();
  }

  public DocPartDataImpl getParentDocPartRow() {
    return parent;
  }

  public List<DocPartDataImpl> getChilds() {
    return childs;
  }

  private void addChild(DocPartDataImpl child) {
    if (childs == null) {
      childs = new ArrayList<>();
    }
    childs.add(child);
  }

  @Override
  public String toString() {
    return metadata.getMetaDocPart().getTableRef().toString();
  }

  @Override
  public Iterator<DocPartRow> iterator() {
    return docPartRows.iterator();
  }

  @Override
  public int fieldColumnsCount() {
    return metadata.getOrdererdFields().size();
  }

  @Override
  public int scalarColumnsCount() {
    return metadata.getOrdererdScalars().size();
  }

  @Override
  public int rowCount() {
    return docPartRows.size();
  }

  @Override
  public Iterator<MetaField> orderedMetaFieldIterator() {
    return metadata.getOrdererdFields().iterator();
  }

  @Override
  public Iterator<MetaScalar> orderedMetaScalarIterator() {
    return metadata.getOrdererdScalars().iterator();
  }

}
