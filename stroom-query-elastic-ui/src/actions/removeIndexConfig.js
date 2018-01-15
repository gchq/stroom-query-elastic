import fetch from 'isomorphic-fetch'

import { sendToSnackbar } from './snackBar'

export const REQUEST_REMOVE_INDEX_CONFIG = 'REQUEST_REMOVE_INDEX_CONFIG'

export const requestRemoveIndexConfig = (apiCallId, uuid) => ({
    type: REQUEST_REMOVE_INDEX_CONFIG,
    uuid,
    apiCallId
})

export const RECEIVE_REMOVE_INDEX_CONFIG = 'RECEIVE_REMOVE_INDEX_CONFIG'

export const receiveRemoveIndexConfig = (apiCallId, uuid) => ({
    type: RECEIVE_REMOVE_INDEX_CONFIG,
    uuid,
    apiCallId
})

export const RECEIVE_REMOVE_INDEX_CONFIG_FAILED = 'RECEIVE_REMOVE_INDEX_CONFIG_FAILED'

export const receiveRemoveIndexConfigFailed = (apiCallId, message) => ({
    type: RECEIVE_REMOVE_INDEX_CONFIG_FAILED,
    message,
    apiCallId
})

let apiCallId = 0

export const removeIndexConfig = (uuid) => {
    return function(dispatch, getState) {
        const thisApiCallId = `removeIndexConfig-${apiCallId}`
        apiCallId += 1

        dispatch(requestRemoveIndexConfig(thisApiCallId, uuid));

        const state = getState()
        const jwsToken = state.authentication.idToken

        return fetch(`${state.config.queryElasticUrl}/docRefApi/v1/delete/${uuid}`,
            {
                headers: {
                    'Accept': 'application/json',
                    'Authorization': 'Bearer ' + jwsToken
                },
                method: "DELETE",
                mode: 'cors'
            }
        )
        .then(
            response => {
                dispatch(receiveRemoveIndexConfig(thisApiCallId, uuid))
                dispatch(sendToSnackbar('Index config deleted'))
            }
        )
        .catch(error => {
            dispatch(receiveRemoveIndexConfigFailed(thisApiCallId, error.message))
            dispatch(sendToSnackbar('Failed to delete index config ' + error.message))
        })
    }
}
