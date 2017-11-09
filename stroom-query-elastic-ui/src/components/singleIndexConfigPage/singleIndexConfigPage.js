import React, { Component } from 'react'
import PropTypes from 'prop-types'

import Paper from 'material-ui/Paper'
import AppBar from 'material-ui/AppBar'
import RaisedButton from 'material-ui/RaisedButton'
import TextField from 'material-ui/TextField'
import Dialog from 'material-ui/Dialog'
import FlatButton from 'material-ui/FlatButton'

import ApiCallSpinner from '../apiCallSpinner'
import ErrorDisplay from '../errorDisplay'
import SnackbarDisplay from '../snackbarDisplay'

import '../appStyle/app.css'
import './singleIndexConfig.css'

class SingleIndexConfigPage extends Component {
    constructor(props) {
        super(props);

        this.state = {
            open: false,
        };
    }

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
    
    handleOpen() {
        this.setState({open: true});
    };

    handleClose() {
        this.setState({open: false});
    };

    handleRemoveAndClose() {
        this.props.removeIndexConfig(this.props.indexConfigUuid)
        this.handleClose();
    }

    render() {
        const removeActions = [
            <FlatButton
                label="Cancel"
                primary={true}
                onClick={this.handleClose.bind(this)}
                />,
            <FlatButton
                label="Remove"
                primary={true}
                onClick={this.handleRemoveAndClose.bind(this)}
                />,
        ];

        // Indicate if the annotation information is clean
        let title = `Index Config ${this.props.indexConfigUuid}`
        if (!this.props.isClean) {
            title += " *"
        }

        let iconElementLeft = <div />

        let nonExistentMessage = undefined;
        if (!this.props.doesExist) {
            nonExistentMessage = <p>This index config does not currently exist</p>
        }

        return (
            <div className='app'>
                <AppBar
                    title={title}
                    iconElementLeft={iconElementLeft}
                    onLeftIconButtonTouchTap={() => this.props.history.push('/')}
                    iconElementRight={<ErrorDisplay />}
                    />
                <SnackbarDisplay />
                <ApiCallSpinner />
                <Paper className='app--body' zDepth={0}>
                    <TextField value={this.props.indexConfig.indexName}
                            onChange={this.onIndexNameChange.bind(this)}
                            hintText="Enter the name of the Elastic Search index"
                            floatingLabelText="Index Name"
                            fullWidth={true}
                        />
                    <TextField value={this.props.indexConfig.indexedType}
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
                                />
                        <RaisedButton
                                label="Remove"
                                onClick={this.handleOpen.bind(this)}
                                className='single-index-config__remove-button'
                                />
                    </div>
                    {nonExistentMessage}

                    <Dialog
                        actions={removeActions}
                        modal={false}
                        open={this.state.open}
                        onRequestClose={this.handleClose.bind(this)}
                    >
                        Remove the Index Config {this.props.indexConfigUuid}?
                    </Dialog>
                </Paper>
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
    getIndexConfig: PropTypes.func.isRequired,
    removeIndexConfig: PropTypes.func.isRequired
}

export default SingleIndexConfigPage