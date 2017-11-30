import {
    EDIT_INDEX_CONFIG,
    REQUEST_UPDATE_INDEX_CONFIG,
    RECEIVE_UPDATE_INDEX_CONFIG,
    RECEIVE_UPDATE_INDEX_CONFIG_FAILED
} from '../actions/updateIndexConfig'

import {
    REQUEST_REMOVE_INDEX_CONFIG,
    RECEIVE_REMOVE_INDEX_CONFIG,
    RECEIVE_REMOVE_INDEX_CONFIG_FAILED
} from '../actions/removeIndexConfig'

import {
    REQUEST_GET_INDEX_CONFIG,
    RECEIVE_GET_INDEX_CONFIG,
    RECEIVE_GET_INDEX_CONFIG_FAILED
} from '../actions/getIndexConfig'

const defaultIndexConfig = {
    indexName: '',
    indexedType: ''
}

const defaultState = {
    isClean: true,
    doesExist: false,
    indexConfig: defaultIndexConfig
}

const singleIndexConfig = (
    state = defaultState,
    action
) => {
    switch (action.type) {
        case EDIT_INDEX_CONFIG:
            return Object.assign({}, state, {
                isClean: false,
                indexConfig: {
                    ...state.indexConfig,
                    ...action.updates
                }
            })
        case REQUEST_GET_INDEX_CONFIG:
        case REQUEST_UPDATE_INDEX_CONFIG:
            return Object.assign({}, state, {
                isClean: false,
            })
        case RECEIVE_GET_INDEX_CONFIG:
        case RECEIVE_UPDATE_INDEX_CONFIG:
            return Object.assign({}, state, {
                isClean: true,
                doesExist: true,
                indexConfig: action.indexConfig
            })
        case RECEIVE_GET_INDEX_CONFIG_FAILED: 
        case RECEIVE_UPDATE_INDEX_CONFIG_FAILED:
        case RECEIVE_REMOVE_INDEX_CONFIG_FAILED:
            return Object.assign({}, state, {
                isClean: false,
                doesExist: false
            })
        case REQUEST_REMOVE_INDEX_CONFIG:
            return Object.assign({}, state, {
                isClean: false 
            })
        case RECEIVE_REMOVE_INDEX_CONFIG:
            return Object.assign({}, state, {
                isClean: true,
                doesExist: false,
                indexConfig: defaultIndexConfig
            })
        default:
            return state
    }
}

export default singleIndexConfig