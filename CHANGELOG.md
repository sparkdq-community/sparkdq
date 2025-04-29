# CHANGELOG


## v0.2.1 (2025-04-29)

### Bug Fixes

- **pyproject.toml**: Remov invalid classifier
  ([#9](https://github.com/sparkdq-community/sparkdq/pull/9),
  [`05f89ee`](https://github.com/sparkdq-community/sparkdq/commit/05f89eedba22cbf346f6a6781abbaab431f8e753))


## v0.2.0 (2025-04-29)

### Continuous Integration

- **push-rule**: Remove branch that was used for testing purpose
  ([`f49bc7f`](https://github.com/sparkdq-community/sparkdq/commit/f49bc7f8fcf695b8afc3e4ea8243601bae5df41c))

### Documentation

- Restructured documentation and improved readability
  ([`53426d0`](https://github.com/sparkdq-community/sparkdq/commit/53426d0d2e1bf3a27cbb81345e374b09f6cb708e))

- **README**: Add marker for documentation
  ([`775912a`](https://github.com/sparkdq-community/sparkdq/commit/775912a51faf3cbb132aca2f255bce78e695c062))

### Features

- Add check column-presence-check
  ([`3e78748`](https://github.com/sparkdq-community/sparkdq/commit/3e787485cb3dbc6b2ebdc4e3c5f6c4cc0c6acbe7))

- Add is-contained-in check
  ([`7639547`](https://github.com/sparkdq-community/sparkdq/commit/7639547c08427ce1c916725b5d1b790460d2254d))

- Add is-not-contained check
  ([`2bc6ca7`](https://github.com/sparkdq-community/sparkdq/commit/2bc6ca72419bfdb336b2a3ef3be75567e1fa334b))

- Add schema check
  ([`043f49c`](https://github.com/sparkdq-community/sparkdq/commit/043f49cf9a5027ae5f44caabbc5ac241765d6bf8))

### Performance Improvements

- Replaced for loops with sql.reduce function
  ([`b178bbb`](https://github.com/sparkdq-community/sparkdq/commit/b178bbbaa8b1e6c5ee7d66e5ebf9270c962b70c9))

### Refactoring

- Add base method to handle results from row level checks
  ([`b41df83`](https://github.com/sparkdq-community/sparkdq/commit/b41df83c8fdea7a787e111852fc04dae84868f03))

### Testing

- Add test with different data types
  ([`1841d94`](https://github.com/sparkdq-community/sparkdq/commit/1841d9407a29e1050c40422735c85807ee0e1272))


## v0.1.2 (2025-04-27)

### Bug Fixes

- **ci**: Add step-id to release step ([#7](https://github.com/sparkdq-community/sparkdq/pull/7),
  [`ba7af39`](https://github.com/sparkdq-community/sparkdq/commit/ba7af3902c5da65c480989d7b0ff83c8d876c9dd))


## v0.1.1 (2025-04-27)

### Bug Fixes

- **dependencies**: Added pydantic dependency
  ([#6](https://github.com/sparkdq-community/sparkdq/pull/6),
  [`958a9ee`](https://github.com/sparkdq-community/sparkdq/commit/958a9ee96c83b93b84c8b8acd77de7681d689c37))


## v0.1.0 (2025-04-27)

### Continuous Integration

- Add basic github workflow ([#3](https://github.com/sparkdq-community/sparkdq/pull/3),
  [`627216c`](https://github.com/sparkdq-community/sparkdq/commit/627216c0300ff4b9592dba67fbe7649b7312cffd))

- Add release workflow job ([#5](https://github.com/sparkdq-community/sparkdq/pull/5),
  [`5736f09`](https://github.com/sparkdq-community/sparkdq/commit/5736f09ce6b830556de06f62606e3a022707e816))

### Features

- Initialize first prototype ([#1](https://github.com/sparkdq-community/sparkdq/pull/1),
  [`8395b66`](https://github.com/sparkdq-community/sparkdq/commit/8395b661a8bd78155d5040a3c15e8a9dc04c7ae1))

* feat: initialized first basic version

* docs: updated introduction in README
