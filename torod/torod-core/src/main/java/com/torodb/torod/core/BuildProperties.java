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

package com.torodb.torod.core;


import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public interface BuildProperties {

    public String getFullVersion();

    public int getMajorVersion();

    public int getMinorVersion();

    public int getSubVersion();

    public String getExtraVersion();

    public long getBuildTime();

    public String getGitCommitId();

    public String getGitBranch();

    public String getGitRemoteOriginURL();

    public String getJavaVersion();

    public String getJavaVendor();

    public String getJavaVMSpecificationVersion();

    public String getJavaVMVersion();

    public String getOsName();

    public String getOsArch();

    public String getOsVersion();
}
