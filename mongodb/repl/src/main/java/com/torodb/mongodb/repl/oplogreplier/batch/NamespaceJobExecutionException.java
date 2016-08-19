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

import com.eightkdata.mongowp.Status;
import com.torodb.core.annotations.DoNotChange;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class NamespaceJobExecutionException extends Exception {

    private static final long serialVersionUID = -6900585949003470244L;

    private final NamespaceJob job;
    private final List<Status<?>> errors;

    public NamespaceJobExecutionException(NamespaceJob job, @DoNotChange List<Status<?>> errors) {
        this.job = job;
        if (!(errors instanceof Serializable)) {
            this.errors = errors;
        } else {
            this.errors = new ArrayList<>(errors);
        }
    }

    public List<Status<?>> getErrors() {
        return errors;
    }

    @Override
    public String getMessage() {
        return "Errors while applying " + job + ". Errors: " + errors;
    }
}
