import {
    REQUEST_UPDATE_INDEX_CONFIG,
    RECEIVE_UPDATE_INDEX_CONFIG,
    RECEIVE_UPDATE_INDEX_CONFIG_FAILED
} from '../actions/updateIndexConfig'

import {
    REQUEST_GET_INDEX_CONFIG,
    RECEIVE_GET_INDEX_CONFIG,
    RECEIVE_GET_INDEX_CONFIG_NOT_EXIST,
    RECEIVE_GET_INDEX_CONFIG_FAILED
} from '../actions/getIndexConfig'

import {
    REQUEST_REMOVE_INDEX_CONFIG,
    RECEIVE_REMOVE_INDEX_CONFIG,
    RECEIVE_REMOVE_INDEX_CONFIG_FAILED
} from '../actions/removeIndexConfig'

const defaultState = []

const apiCalls = (
    state = defaultState,
    action
 ) => {
    switch(action.type) {
        case REQUEST_UPDATE_INDEX_CONFIG:
        case REQUEST_GET_INDEX_CONFIG:
        case REQUEST_REMOVE_INDEX_CONFIG:
            return [
                ...state,
                {
                    type: action.type,
                    apiCallId: action.apiCallId
                }
            ]
        case RECEIVE_UPDATE_INDEX_CONFIG:
        case RECEIVE_UPDATE_INDEX_CONFIG_FAILED:
        case RECEIVE_GET_INDEX_CONFIG:
        case RECEIVE_GET_INDEX_CONFIG_NOT_EXIST:
        case RECEIVE_GET_INDEX_CONFIG_FAILED:
        case RECEIVE_REMOVE_INDEX_CONFIG:
        case RECEIVE_REMOVE_INDEX_CONFIG_FAILED:
            return state.filter(a => a.apiCallId !== action.apiCallId)
        default:
            return state
    }
}

export default apiCalls