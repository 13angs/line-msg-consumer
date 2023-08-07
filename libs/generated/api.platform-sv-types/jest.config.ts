/* eslint-disable */
export default {
    displayName: 'generated-api.platform-sv-types',
    preset: '../../../jest.preset.js',
    transform: {
        '^.+\\.[tj]s$': [
            'ts-jest',
            { tsconfig: '<rootDir>/tsconfig.spec.json' },
        ],
    },
    moduleFileExtensions: ['ts', 'js', 'html'],
    coverageDirectory: '../../../coverage/libs/generated/api.platform-sv-types',
};