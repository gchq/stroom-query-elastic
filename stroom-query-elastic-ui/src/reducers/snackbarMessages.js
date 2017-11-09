import {
    RECEIVE_REMOVE_INDEX_CONFIG
} from '../actions/removeIndexConfig'

import {
    RECEIVE_UPDATE_INDEX_CONFIG
} from '../actions/updateIndexConfig'

import {
    ACKNOWLEDGE_SNACKBAR,
    GENERIC_SNACKBAR
} from '../actions/acknowledgeApiMessages'

const defaultState = []

let messageId = 0

const snackbarMessages = (
    state = defaultState,
    action
 ) => {
    const generateNewState = (message) => {
        const newState = [
            ...state,
            {
                messageId,
                message,
                receivedAt: action.receivedAt
            }
        ]
        messageId += 1
        return newState
    }

    switch(action.type) {
        case RECEIVE_UPDATE_INDEX_CONFIG:
            return generateNewState("Index Config Updated")
        case RECEIVE_REMOVE_INDEX_CONFIG:
            return generateNewState("Index Config Removed")
        case GENERIC_SNACKBAR:
            return generateNewState(action.message)
        case ACKNOWLEDGE_SNACKBAR:
            return state.filter(message => action.id !== message.id)
        default:
            return state
    }
}

export default snackbarMessages