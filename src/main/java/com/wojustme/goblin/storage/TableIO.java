package com.wojustme.goblin.storage;

import org.apache.arrow.vector.FieldVector;

import java.util.List;

/** Interface for reading and writing table. */
public interface TableIO {

  void write(List<FieldVector> valueVectors);

  List<FieldVector> readColumns(String... col);

}
