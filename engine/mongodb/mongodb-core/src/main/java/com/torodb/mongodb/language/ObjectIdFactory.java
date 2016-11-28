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

package com.torodb.mongodb.language;

import com.eightkdata.mongowp.bson.BsonObjectId;
import com.eightkdata.mongowp.bson.impl.IntBasedBsonObjectId;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.UnsignedInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public class ObjectIdFactory {

  private static final Logger LOGGER =
      LogManager.getLogger(ObjectIdFactory.class);

  private static final int MACHINE_ID = createMachineId();
  private static final int PROCESS_ID = createProcessId();
  private static final AtomicInteger COUNTER = new AtomicInteger(new SecureRandom().nextInt());

  public BsonObjectId consumeObjectId() {
    long secs = System.currentTimeMillis() / 1000;
    return new IntBasedBsonObjectId(
        UnsignedInteger.valueOf(secs).intValue(),
        MACHINE_ID,
        PROCESS_ID,
        COUNTER.getAndIncrement() & 0xFFFFFF
    );
  }

  private static int createMachineId() {
    int machineId;
    try {
      Hasher hasher = Hashing.crc32c().newHasher();
      Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
      boolean atLeastOne = false;
      while (nics.hasMoreElements()) {
        NetworkInterface ni = nics.nextElement();
        if (ni != null) {
          byte[] macAddress = ni.getHardwareAddress();
          if (macAddress != null) {
            for (byte b : macAddress) {
              atLeastOne = true;
              hasher.putByte(b);
            }
          }
        }
      }
      if (!atLeastOne) {
        LOGGER.warn("Failed to calculate the machine id. A random number is used");
        machineId = new SecureRandom().nextInt();
      } else {
        machineId = hasher.hash().asInt();
      }
    } catch (SocketException ex) {
      LOGGER.warn("Failed to calculate the machine id. A random number is used");
      machineId = new SecureRandom().nextInt();
    }

    return machineId & 0xFFFFFF;
  }

  private static int createProcessId() {
    int pid;
    String name = ManagementFactory.getRuntimeMXBean().getName();
    try {
      pid = Integer.parseInt(name.substring(0, name.indexOf("@")));
    } catch (Throwable ex) {
      LOGGER.warn("Failed to calculate the process id. A random number is used");
      pid = new SecureRandom().nextInt();
    }
    return pid & 0xFFFF;
  }

}
