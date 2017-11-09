import React from 'react';
import { render } from 'react-dom'
import thunkMiddleware from 'redux-thunk'
import { createLogger } from 'redux-logger'
import { Provider } from 'react-redux'

import {
    createStore,
    applyMiddleware
} from 'redux'

import {
    BrowserRouter as Router,
    Route,
    Switch
} from 'react-router-dom'

import {blue600, amber900} from 'material-ui/styles/colors'
import MuiThemeProvider from 'material-ui/styles/MuiThemeProvider';
import getMuiTheme from 'material-ui/styles/getMuiTheme'

import SingleIndexConfigPage from './components/singleIndexConfigPage'
import NotFoundPage from './components/notFoundPage'

import reducer from './reducers'

const loggerMiddleware = createLogger()

const theme = getMuiTheme({
    palette: {
        primary1Color: blue600,
        accent1Color: amber900,
    }
})

const store = createStore(
    reducer,
    applyMiddleware(
        thunkMiddleware, // lets us dispatch() functions
        loggerMiddleware // neat middleware that logs actions
    )
)

const SinglePage = ({ match }) => {
    return <SingleIndexConfigPage indexConfigUuid={match.params.uuid}/>
}

render(
    <MuiThemeProvider muiTheme={theme}>
        <Provider store={store}>
            <Router>
                <Switch>
                    <Route exact={true} path="/:uuid" component={SinglePage} />
                    <Route path="*" component={NotFoundPage} />
                </Switch>
            </Router>
        </Provider>
    </MuiThemeProvider>,
    document.getElementById('root')
)

