/*
 * ToroDB - ToroDB: MongoDB Repl
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.mongodb.repl.oplogreplier.batch;

/**
 *
 */
public interface AnalyzedOplogBatchVisitor<Result, Arg, T extends Throwable> {

    public Result visit(SingleOpAnalyzedOplogBatch batch, Arg arg) throws T;

    public Result visit(CudAnalyzedOplogBatch batch, Arg arg) throws T;

}