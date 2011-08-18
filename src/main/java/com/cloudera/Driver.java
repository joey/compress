package com.cloudera;

import org.apache.hadoop.util.ProgramDriver;

public class Driver {

  public static void main(String argv[]) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("compress", Compress.class,
              "MapReduce program to read uncompressed text files and output a compressed version of the files.");
      pgd.driver(argv);

      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }

    System.exit(exitCode);
  }
}
