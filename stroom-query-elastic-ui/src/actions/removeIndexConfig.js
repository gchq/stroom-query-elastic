import fetch from 'isomorphic-fetch'

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
    return function(dispatch) {
        const thisApiCallId = `removeIndexConfig-${apiCallId}`
        apiCallId += 1

        dispatch(requestRemoveIndexConfig(thisApiCallId, uuid));

        return fetch(`${process.env.REACT_APP_QUERY_ELASTIC_URL}/explorerAction/v1/${uuid}`,
            {
                method: "DELETE"
            }
        )
        .then(
            response => {
                dispatch(receiveRemoveIndexConfig(thisApiCallId, uuid))
            }
        )
        .catch(error => {
            dispatch(receiveRemoveIndexConfigFailed(thisApiCallId, error.message))
        })
    }
}
