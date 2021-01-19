module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    "header-max-length": async () => [2, "always", 50],
    "body-max-line-length": async () => [2, "always", 72],
    'type-enum': [2, 'always', ['build', 'chore', 'ci', 'docs', 'feat', 'fix', 'perf', 'refactor', 'revert', 'style', 'test', 'example']],
  },
  defaultIgnores: false,
}
