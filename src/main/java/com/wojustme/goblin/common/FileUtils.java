package com.wojustme.goblin.common;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;

public class FileUtils {

  public static void createDirectory(String path) {
    final File file = org.apache.commons.io.FileUtils.getFile(path);
    if (!dirExist(file)) {
      forceMkdir(file);
    }
  }

  public static void forceMkdir(File dir) {
    try {
      org.apache.commons.io.FileUtils.forceMkdir(dir);
    } catch (IOException e) {
      throw new GoblinRuntimeException(e, "Create directory %s fail.", dir);
    }
  }

  public static boolean dirExist(File dir) {
    if (dir.exists()) {
      Preconditions.checkArgument(
          dir.isDirectory(), "Path %s exist, but it's not a directory.", dir);
      return true;
    } else {
      return false;
    }
  }
}
