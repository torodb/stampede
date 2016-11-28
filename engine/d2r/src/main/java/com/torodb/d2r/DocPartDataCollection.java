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

package com.torodb.d2r;

import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;
import com.torodb.d2r.model.DocPartDataImpl;
import com.torodb.d2r.model.PathStack.PathInfo;
import com.torodb.d2r.model.TableMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocPartDataCollection implements CollectionData {

  private final Map<PathInfo, DocPartDataImpl> docPartDataMap = new HashMap<>();
  private final List<DocPartDataImpl> docPartDataList = new ArrayList<>();
  private final CollectionMetaInfo collectionMetaInfo;

  public DocPartDataCollection(CollectionMetaInfo collectionMetaInfo) {
    this.collectionMetaInfo = collectionMetaInfo;
  }

  public DocPartDataImpl findDocPartData(PathInfo path) {
    DocPartDataImpl docPartData = docPartDataMap.get(path);
    if (docPartData == null) {
      TableMetadata metadata = new TableMetadata(collectionMetaInfo, path.getTableRef());
      DocPartDataImpl parentDocPartData = findParent(path);
      docPartData = new DocPartDataImpl(metadata, parentDocPartData);
      docPartDataMap.put(path, docPartData);
      docPartDataList.add(docPartData);
    }
    return docPartData;
  }

  private DocPartDataImpl findParent(PathInfo path) {
    PathInfo it = path.getParent();
    while (it != null) {
      DocPartDataImpl tableInfo = docPartDataMap.get(it);
      if (tableInfo != null) {
        return tableInfo;
      }
      it = it.getParent();
    }
    return null;
  }

  @Override
  public Iterable<DocPartData> orderedDocPartData() {
    List<DocPartData> all = new ArrayList<>();
    for (DocPartDataImpl table : docPartDataList) {
      if (table.getParentDocPartRow() == null) {
        all.add(table);
        addChilds(table, all);
        return all;
      }
    }
    return all;
  }

  private void addChilds(DocPartDataImpl current, List<DocPartData> all) {
    List<DocPartDataImpl> childs = current.getChilds();
    if (childs != null && childs.size() > 0) {
      all.addAll(childs);
      for (DocPartDataImpl child : childs) {
        addChilds(child, all);
      }
    }
  }
}
