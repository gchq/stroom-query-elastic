import React, { Component } from 'react'
import { connect } from 'react-redux'

import {
    BrowserRouter,
    Route,
    Switch,
    withRouter
} from 'react-router-dom'

import SingleIndexConfig from '../singleIndexConfig'
import NotFound from '../notFound'

import { AuthenticationRequest, HandleAuthenticationResponse } from 'stroom-js'

class App extends Component {

    isLoggedIn() {
        return !!this.props.idToken
    }

    render() {
        return (
            <div>
                <BrowserRouter basename={'/'} />
                <Switch>
                    <Route exact path={'/handleAuthenticationResponse'} render={() => (<HandleAuthenticationResponse
                        authenticationServiceUrl={this.props.authenticationServiceUrl}
                        authorisationServiceUrl={this.props.authorisationServiceUrl} />
                    )} />
                    <Route exact={true} path="/:uuid" render={(route) => (
                        this.isLoggedIn() ?
                            <SingleIndexConfig indexConfigUuid={route.match.params.uuid} /> :
                            <AuthenticationRequest
                                referrer={route.location.pathname}
                                uiUrl={this.props.advertisedUrl}
                                appClientId={this.props.appClientId}
                                authenticationServiceUrl={this.props.authenticationServiceUrl} />

                    )} />

                    <Route path="*" render={(route) => (
                        this.isLoggedIn() ?
                            <NotFound /> :
                            <AuthenticationRequest
                                referrer={route.location.pathname}
                                uiUrl={this.props.advertisedUrl}
                                appClientId={this.props.appClientId}
                                authenticationServiceUrl={this.props.authenticationServiceUrl} />
                    )} />
                </Switch>
            </div>
        )
    }
}

export default withRouter(connect(
    (state) => ({
        idToken: state.authentication.idToken,
        advertisedUrl: state.config.advertisedUrl,
        appClientId: state.config.appClientId,
        authenticationServiceUrl: state.config.authenticationServiceUrl,
        authorisationServiceUrl: state.config.authorisationServiceUrl
    }),
    {
    }
)(App));