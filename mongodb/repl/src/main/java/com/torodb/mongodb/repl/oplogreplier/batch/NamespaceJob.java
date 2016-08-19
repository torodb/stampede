/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.oplogreplier.batch;

import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOp;
import java.util.Collection;

/**
 *
 */
public class NamespaceJob {

    private final String database;
    private final String collection;
    private final Collection<AnalyzedOp> jobs;

    public NamespaceJob(String database, String collection, Collection<AnalyzedOp> jobs) {
        this.database = database;
        this.collection = collection;
        this.jobs = jobs;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public Collection<AnalyzedOp> getJobs() {
        return jobs;
    }

    @Override
    public String toString() {
        return "NamespaceJob on "+ database + '.' + collection + ": " +
                jobs;
    }

}
