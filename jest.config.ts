import type { Config } from 'jest';

const config: Config = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1',
    '^@miyabi/shared$': '<rootDir>/shared/src/index.ts',
  },
  transform: {
    '^.+\\.tsx?$': [
      'ts-jest',
      {
        tsconfig: {
          target: 'ES2022',
          module: 'CommonJS', // Force ts-jest to compile tests to CommonJS for Jest
          lib: ['ES2022'],
          strict: true,
          esModuleInterop: true,
          isolatedModules: true,
          rootDir: '.',
          resolveJsonModule: true,
        },
      },
    ],
  },
  testMatch: ['**/__tests__/**/*.test.ts'],
};

export default config;
