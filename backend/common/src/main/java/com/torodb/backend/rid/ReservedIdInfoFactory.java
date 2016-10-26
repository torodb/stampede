package com.torodb.backend.rid;

import com.google.common.util.concurrent.Service;
import com.torodb.core.TableRef;

public interface ReservedIdInfoFactory extends Service {

	ReservedIdInfo create(String dbName, String collectionName, TableRef tableRef);

}
