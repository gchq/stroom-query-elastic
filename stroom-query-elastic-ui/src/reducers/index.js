import { combineReducers } from 'redux'
import { routerReducer } from 'react-router-redux'

import { authenticationReducer as authentication, authorisationReducer as authorisation } from 'stroom-js'

import singleIndexConfig from './singleIndexConfig'
import snackbarMessages from './snackbarMessages'

export default combineReducers({
    routing: routerReducer,
    config : (state= {}) => state,
    authentication,
    authorisation,
    singleIndexConfig,
    snackbarMessages
})
