import { connect } from 'react-redux'
import { withRouter } from 'react-router'

import SingleIndexConfig from './singleIndexConfig'

import { getIndexConfig } from '../../actions/getIndexConfig'
import { editIndexConfig, updateIndexConfig } from '../../actions/updateIndexConfig'

export default connect(
    (state) => ({
        ...state.singleIndexConfig
    }),
    {
        updateIndexConfig,
        editIndexConfig,
        getIndexConfig
    }
)(withRouter(SingleIndexConfig));
