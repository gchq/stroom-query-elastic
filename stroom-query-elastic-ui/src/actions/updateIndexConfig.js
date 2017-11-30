import fetch from 'isomorphic-fetch'

export const EDIT_INDEX_CONFIG = 'EDIT_INDEX_CONFIG'

export const editIndexConfig = (uuid, updates) => ({
    type: EDIT_INDEX_CONFIG,
    uuid,
    updates
})

export const REQUEST_UPDATE_INDEX_CONFIG = 'REQUEST_UPDATE_INDEX_CONFIG'

export const requestUpdateIndexConfig = (apiCallId, uuid, indexConfig) => ({
    type: REQUEST_UPDATE_INDEX_CONFIG,
    uuid,
    indexConfig,
    apiCallId
})

export const RECEIVE_UPDATE_INDEX_CONFIG = 'RECEIVE_UPDATE_INDEX_CONFIG';
 
export const receiveUpdateIndexConfig = (apiCallId, uuid, indexConfig) => ({
    type: RECEIVE_UPDATE_INDEX_CONFIG,
    uuid,
    indexConfig,
    apiCallId
})

export const RECEIVE_UPDATE_INDEX_CONFIG_FAILED = 'RECEIVE_UPDATE_INDEX_CONFIG_FAILED';

export const receiveUpdateIndexConfigFailed = (apiCallId, message) => ({
    type: RECEIVE_UPDATE_INDEX_CONFIG_FAILED,
    apiCallId,
    message
})

let apiCallId = 0

export const updateIndexConfig = (uuid, indexConfig) => {
    return function(dispatch) {
        const thisApiCallId = `updateIndexConfig-${apiCallId}`
        apiCallId += 1

        dispatch(requestUpdateIndexConfig(thisApiCallId, uuid, indexConfig));

        return fetch(`${process.env.REACT_APP_QUERY_ELASTIC_URL}/elasticIndex/v1/${uuid}`,
            {
                method: "PUT",
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(indexConfig)
            }
        )
        .then(
            response => response.json()
        )
        .then(json => {
            dispatch(receiveUpdateIndexConfig(thisApiCallId, uuid, json))
        })
        .catch(error => {
            dispatch(receiveUpdateIndexConfigFailed(thisApiCallId, error.message))
        })
    }
}
