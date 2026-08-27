// ioBroker eslint template configuration file for js and ts files
// Please note that esm or react based modules need additional modules loaded.
import config from '@iobroker/eslint-config';

export default [
    ...config,
    {
        // specify files to exclude from linting here
        ignores: [
            'test/**/*',
            '*.config.mjs',
            'build/**/*',
            'admin/**/*',
            '**/adapter-config.d.ts',
            'src-admin/**/*',
            'tmp/**/*',
            'tasks.ts',
        ],
    },
    {
        // disable temporary the rule 'jsdoc/require-param' and enable 'jsdoc/require-jsdoc'
        rules: {
            'jsdoc/require-jsdoc': 'off',
            'jsdoc/require-param': 'off',
            '@typescript-eslint/no-require-imports': 'off',
        },
    },
];
