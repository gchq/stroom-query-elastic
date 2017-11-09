import { connect } from 'react-redux'
import { withRouter } from 'react-router'

import SingleIndexConfigPage from './singleIndexConfigPage'

import { getIndexConfig } from '../../actions/getIndexConfig'
import { editIndexConfig, updateIndexConfig } from '../../actions/updateIndexConfig'
import { removeIndexConfig } from '../../actions/removeIndexConfig'

export default connect(
    (state) => ({
        ...state.singleIndexConfig
    }),
    {
        updateIndexConfig,
        editIndexConfig,
        getIndexConfig,
        removeIndexConfig
    }
)(withRouter(SingleIndexConfigPage));
