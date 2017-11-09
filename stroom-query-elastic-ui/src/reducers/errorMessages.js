import {
    RECEIVE_UPDATE_INDEX_CONFIG_FAILED
} from '../actions/updateIndexConfig'

import {
    RECEIVE_GET_INDEX_CONFIG_FAILED
} from '../actions/getIndexConfig'

import {
    RECEIVE_REMOVE_INDEX_CONFIG_FAILED
} from '../actions/removeIndexConfig'

import {
    ACKNOWLEDGE_ERROR,
    GENERIC_ERROR
} from '../actions/acknowledgeApiMessages'

const defaultState = []

let id = 0

const errorMessages = (
    state = defaultState,
    action
 ) => {
    const generateNewState = (userFriendlyType) => {
        const newState = [
            ...state,
            {
                id,
                action: userFriendlyType,
                message: action.message
            }
        ]
        id += 1
        return newState
    }

    switch(action.type) {

        case RECEIVE_UPDATE_INDEX_CONFIG_FAILED:
            return generateNewState('Failed to Update Index Config')
        case RECEIVE_GET_INDEX_CONFIG_FAILED:
            return generateNewState('Failed to Get Index Config')
        case RECEIVE_REMOVE_INDEX_CONFIG_FAILED:
            return generateNewState('Failed to Remove Index Config')
        case GENERIC_ERROR:
            return generateNewState(action.message)
        case ACKNOWLEDGE_ERROR:
            return state.filter(error => action.id !== error.id)
        default:
            return state
    }
}

export default errorMessages