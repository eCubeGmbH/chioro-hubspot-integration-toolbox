# CLAUDE.md

## CRITICAL RULES - READ BEFORE ANY ACTION

1. **Do NOT write tests** - The test infrastructure doesn't work with GraalVM runtime
2. **Do NOT run `npm install` or `npm run`** - There is no point, it will fail
3. **Do NOT try to deploy or verify plugins** - User must do this manually
4. **Focus ONLY on writing plugin code**

## Detailed Documentation

For full plugin development documentation (API patterns, function signatures, examples, available globals, etc.), read AGENT.md in this directory.
