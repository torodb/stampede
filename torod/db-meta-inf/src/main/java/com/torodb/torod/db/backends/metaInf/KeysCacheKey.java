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


package com.torodb.torod.db.backends.metaInf;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 *
 */
class KeysCacheKey {
    private final int fastHash;
    private final int hash;

    public KeysCacheKey(List<String> keys) {
        this.fastHash = getFastHash(keys);
        this.hash = getHash(keys);
    }

    private static int getFastHash(List<String> keys) {
        int result = 0;
        for (String key : keys) {
            result += 10000 + key.length();
        }
        return result;
    }

    private static int getHash(List<String> keys) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Charset charset = Charset.forName("UTF-8");
            for (String key : keys) {
                md.update(key.getBytes(charset));
            }
            byte[] digest = md.digest();
            
            int result = 0;
            for (int i = 0; i < 4 && i < digest.length; i++) {
                result += (digest[i] & 0xFFFF) << i;
            }
            return result;
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError("This should not happen");
        }
    }

    public int getFastHash() {
        return fastHash;
    }

    public int getHash() {
        return hash;
    }

    @Override
    public int hashCode() {
        int myHash = 3;
        myHash = 29 * myHash + this.fastHash;
        myHash = 29 * myHash + this.hash;
        return myHash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final KeysCacheKey other = (KeysCacheKey) obj;
        if (this.fastHash != other.fastHash) {
            return false;
        }
        if (this.hash != other.hash) {
            return false;
        }
        return true;
    }
    
}
