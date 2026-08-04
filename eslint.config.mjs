// ioBroker eslint template configuration file for js and ts files
// Please note that esm or react based modules need additional modules loaded.
import config from '@iobroker/eslint-config';

export default [
    ...config,

    {
        // specify files to exclude from linting here
        ignores: [
            '*.test.js',
            'test/**/*.js',
            '*.config.mjs',
            'build/**/*',
            'admin/build',
            'admin/words.js',
            'admin/admin.d.ts',
            '**/adapter-config.d.ts',

            // these files need to be adapted in the future
            'admin/blockly.js',
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
