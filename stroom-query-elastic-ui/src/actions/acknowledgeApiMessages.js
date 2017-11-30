export const ACKNOWLEDGE_SNACKBAR = 'ACKNOWLEDGE_SNACKBAR'

export const acknowledgeSnackbar = (id) => ({
    type: ACKNOWLEDGE_SNACKBAR,
    id
})

export const GENERIC_SNACKBAR = 'GENERIC_SNACKBAR'

export const genericSnackbar = (message) => ({
    type: GENERIC_SNACKBAR,
    message
})