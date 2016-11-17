package com.threeandahalfroses.aws.ddb.loader;

import org.json.simple.JSONObject;

/**
 * @author patrizz on 16/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class LoaderState {

    private final JSONObject stateVariables;

    enum StateName {
        OFF,
        STARTING,
        DIFFS_DETERMINED_AND_SAVED,
        DELETED_ITEMS_REMOVED_FROM_DYNAMODB,
        DELETED_DIFF_FILL,
        PROCESSING_FILE,
        FILE_PROCESSED
    }

    private StateName stateName = StateName.OFF;

    LoaderState(StateName stateName, JSONObject stateVariables) {
        this.stateName = stateName;
        this.stateVariables = stateVariables;
    }

    public StateName getStateName() {
        return stateName;
    }

    public JSONObject getStateVariables() {
        return stateVariables;
    }



}