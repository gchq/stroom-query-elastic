import React, { Component } from 'react'
import PropTypes from 'prop-types'

import Paper from 'material-ui/Paper'
import RaisedButton from 'material-ui/RaisedButton'
import TextField from 'material-ui/TextField'

import ApiCallSpinner from '../apiCallSpinner'
import SnackbarDisplay from '../snackbarDisplay'

import '../appStyle/app.css'
import './singleIndexConfig.css'

class SingleIndexConfigPage extends Component {

    componentDidMount() {
        this.props.getIndexConfig(this.props.indexConfigUuid)
    }

    onIndexNameChange(e) {
        const updates = {
            indexName: e.target.value
        }

        this.props.editIndexConfig(this.props.indexConfigUuid, updates)
    }

    onIndexedTypeChange(e) {
        const updates = {
            indexedType: e.target.value
        }

        this.props.editIndexConfig(this.props.indexConfigUuid, updates)
    }
    
    saveChanges() {
        this.props.updateIndexConfig(this.props.indexConfigUuid, this.props.indexConfig)
    }

    render() {
        let mainContent = undefined;

        let indexNameValue = (!!this.props.indexConfig.indexName) ? this.props.indexConfig.indexName : ""
        let indexedTypeValue = (!!this.props.indexConfig.indexedType) ? this.props.indexConfig.indexedType : ""

        if (this.props.doesExist) {
            mainContent = (
                <Paper className='app--body' zDepth={0}>
                    <TextField value={indexNameValue}
                            onChange={this.onIndexNameChange.bind(this)}
                            hintText="Enter the name of the Elastic Search index"
                            floatingLabelText="Index Name"
                            fullWidth={true}
                        />
                    <TextField value={indexedTypeValue}
                            onChange={this.onIndexedTypeChange.bind(this)}
                            hintText="Enter the name of the Elastic Search indexed type"
                            floatingLabelText="Indexed Type"
                            fullWidth={true}
                        />
                    <div>
                        <RaisedButton
                            label="Save Changes"
                            onClick={this.saveChanges.bind(this)}
                            primary={true}
                            className='single-index-config__save-button'
                            disabled={this.props.isClean}
                            />
                    </div>
                </Paper>
            )
        } else {
            mainContent = (
                <Paper className='app--body' zDepth={0}>
                    <p>This index config does not currently exist</p>
                </Paper>
            )
        }

        return (
            <div className='app'>
                <SnackbarDisplay />
                <ApiCallSpinner />
                {mainContent}
            </div>
        )
    }
}

SingleIndexConfigPage.propTypes = {
    // Set from routing
    indexConfigUuid: PropTypes.string.isRequired,

    // Set by react router
    history: PropTypes.object.isRequired,

    // Connected to redux state
    indexConfig: PropTypes.object.isRequired,
    isClean: PropTypes.bool.isRequired,
    doesExist: PropTypes.bool.isRequired,

    // Connected to redux actions
    editIndexConfig: PropTypes.func.isRequired,
    updateIndexConfig: PropTypes.func.isRequired,
    getIndexConfig: PropTypes.func.isRequired
}

export default SingleIndexConfigPage