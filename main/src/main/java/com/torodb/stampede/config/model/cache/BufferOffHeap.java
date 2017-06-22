package com.torodb.stampede.config.model.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.mongodb.repl.oplogreplier.config.BufferOffHeapConfig;
import com.torodb.mongodb.repl.oplogreplier.config.BufferRollCycle;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.util.ConfigUtils;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

@Description("config.bufferOffHeap")
@JsonPropertyOrder({"enabled", "path", "maxSize", "rollCycle"})
public class BufferOffHeap implements BufferOffHeapConfig {

  @Description("config.bufferOffHeap.enabled")
  @JsonProperty(required = true)
  private Boolean enabled;
  @Description("config.bufferOffHeap.path")
  private String path;
  @Description("config.bufferOffHeap.maxsize")
  private int maxSize;
  @Description("config.bufferOffHeap.rollcycle")
  private BufferRollCycle rollCycle;

  public BufferOffHeap() {
    enabled = false;
    path = ConfigUtils.getDefaultTempPath();
    rollCycle = BufferRollCycle.DAILY;
  }

  @Override
  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public int getMaxSize() {
    return maxSize;
  }

  public void setMaxSize(int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public BufferRollCycle getRollCycle() {
    return rollCycle;
  }

  public void setRollCycle(BufferRollCycle rollCycle) {
    this.rollCycle = rollCycle;
  }


  private static void deleteOnClose(Path path) {
    Runnable runnable = () -> deleteFolder(path);
    Runtime.getRuntime().addShutdownHook(new Thread(runnable, "deleteOnClose-" + path));
  }

  @SuppressWarnings("checkstyle:EmptyCatchBlock")
  private static void deleteFolder(Path path) {
    try {
      Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException ignored) {
    }
  }
}
