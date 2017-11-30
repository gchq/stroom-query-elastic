import { combineReducers } from 'redux'

import singleIndexConfig from './singleIndexConfig'
import apiCalls from './apiCalls'
import snackbarMessages from './snackbarMessages'

export default combineReducers({
    singleIndexConfig,
    snackbarMessages,
    apiCalls
})
