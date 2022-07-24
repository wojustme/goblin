package com.wojustme.goblin.common;

import com.wojustme.goblin.fun.FunctionRegistry;
import com.wojustme.goblin.storage.DataStorageManager;

/**
 * Single Goblin context instance.
 */
public record GoblinContext(DataStorageManager storageManager, FunctionRegistry functionRegistry) {

}
