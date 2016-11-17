package com.threeandahalfroses.aws.ddb.loader;

import java.util.List;

/**
 * @author patrizz on 10/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class Diffs {

    List<String> deletedFileIds;

    public Diffs(List<String> deletedFileIds) {
        this.deletedFileIds = deletedFileIds;
    }

    public List<String> getDeletedFileIds() {
        return deletedFileIds;
    }
}
