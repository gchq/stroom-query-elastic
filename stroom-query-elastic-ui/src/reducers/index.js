import { combineReducers } from 'redux'

import singleIndexConfig from './singleIndexConfig'
import apiCalls from './apiCalls'
import snackbarMessages from './snackbarMessages'
import errorMessages from './errorMessages'

export default combineReducers({
    singleIndexConfig,
    snackbarMessages,
    errorMessages,
    apiCalls
})
