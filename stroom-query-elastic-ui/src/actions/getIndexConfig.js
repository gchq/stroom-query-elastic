import fetch from 'isomorphic-fetch'

export const REQUEST_GET_INDEX_CONFIG = 'REQUEST_GET_INDEX_CONFIG'

export const requestGetIndexConfig = (apiCallId, uuid) => ({
    type: REQUEST_GET_INDEX_CONFIG,
    uuid,
    apiCallId
})

export const RECEIVE_GET_INDEX_CONFIG = 'RECEIVE_GET_INDEX_CONFIG'

export const receiveGetIndexConfig = (apiCallId, uuid, json) => ({
    type: RECEIVE_GET_INDEX_CONFIG,
    uuid,
    indexConfig: json,
    apiCallId
})

export const RECEIVE_GET_INDEX_CONFIG_FAILED = 'RECEIVE_GET_INDEX_CONFIG_FAILED'

export const receiveGetIndexConfigFailed = (apiCallId, message) => ({
    type: RECEIVE_GET_INDEX_CONFIG_FAILED,
    apiCallId,
    message
})

let apiCallId = 0;

export const getIndexConfig = (uuid) => {
    return function(dispatch) {
        const thisApiCallId = `getIndexConfig-${apiCallId}`
        apiCallId += 1

        dispatch(requestGetIndexConfig(thisApiCallId, uuid))

        return fetch(`${process.env.REACT_APP_QUERY_ELASTIC_URL}/docRefApi/v1/${uuid}`)
        .then(
            response => {
                if (!response.ok) {
                    throw new Error(response.statusText)
                }
                return response.json()
            }
        )
        .then(json => {
            if (json.uuid) {
                dispatch(receiveGetIndexConfig(thisApiCallId, uuid, json))
            }
        })
        .catch(error => {
            dispatch(receiveGetIndexConfigFailed(thisApiCallId, error.message))
        })
    }
}