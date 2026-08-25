import config from '@iobroker/eslint-config';

export default [
    ...config,
    {
        ignores: ['build/**/*', '*.config.mjs', 'vite.config.ts'],
    },
    {
        rules: {
            'jsdoc/require-jsdoc': 'off',
            'jsdoc/require-param': 'off',
        },
    },
];
