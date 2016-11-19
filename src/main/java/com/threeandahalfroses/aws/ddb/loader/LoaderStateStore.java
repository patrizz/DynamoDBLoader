package com.threeandahalfroses.aws.ddb.loader;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author patrizz on 16/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public interface LoaderStateStore {
    LoaderState loadLatest();
    void save(LoaderState loaderState) throws IOException;
    void save(LoaderState loaderState, boolean forceToPermanentStorage) throws IOException;
    void forceToPermanentStorage() throws IOException;
}
