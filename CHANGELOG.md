# CHANGELOG


## v0.11.0 (2025-08-09)

### Features

- Make pyspark an optional dependency for Databricks compatibility & update docstrings
  ([#55](https://github.com/sparkdq-community/sparkdq/pull/55),
  [`5c8c030`](https://github.com/sparkdq-community/sparkdq/commit/5c8c03019a30ed82ce4afcd3eff28f5dd7f30585))


## v0.10.0 (2025-06-01)

### Features

- **engine**: Add support for reference-based integrity checks
  ([#51](https://github.com/sparkdq-community/sparkdq/pull/51),
  [`f33565b`](https://github.com/sparkdq-community/sparkdq/commit/f33565bf8420ea662a98e5e6b429f32545dfe29d))


## v0.9.0 (2025-05-28)

### Features

- Add ColumnGreaterThanCheck with validation logic and configuration
  ([#48](https://github.com/sparkdq-community/sparkdq/pull/48),
  [`e46734e`](https://github.com/sparkdq-community/sparkdq/commit/e46734eee7c380fa3724885efcb269d282aae29e))


## v0.8.1 (2025-05-27)

### Bug Fixes

- **deps**: Set minimum required Pydantic version to 2.8.0
  ([#47](https://github.com/sparkdq-community/sparkdq/pull/47),
  [`8fe75ab`](https://github.com/sparkdq-community/sparkdq/commit/8fe75ab1210df6b3c17c540fdcf3e01c8e69a515))


## v0.8.0 (2025-05-27)

### Features

- Update ColumnLessThanCheck to use a limit expression
  ([#45](https://github.com/sparkdq-community/sparkdq/pull/45),
  [`06df3b3`](https://github.com/sparkdq-community/sparkdq/commit/06df3b34cde4b70cf2ccdc0076aef9de6ff0125c))


## v0.7.1 (2025-05-25)

### Continuous Integration

- **dependabot**: Add ecosystem for actions
  ([#39](https://github.com/sparkdq-community/sparkdq/pull/39),
  [`15e0782`](https://github.com/sparkdq-community/sparkdq/commit/15e078281249a8c0d2089071a8ad4af1f43934b7))

### Refactoring

- **checks**: Dynamically register CheckConfig classes in __init__.py
  ([#43](https://github.com/sparkdq-community/sparkdq/pull/43),
  [`33023a8`](https://github.com/sparkdq-community/sparkdq/commit/33023a8039e9c5b51091d3941a823261adefaf57))


## v0.7.0 (2025-05-21)

### Features

- Add data freshness check ([#38](https://github.com/sparkdq-community/sparkdq/pull/38),
  [`f0d2318`](https://github.com/sparkdq-community/sparkdq/commit/f0d2318486c55f381a626acda6be68fd48adce45))


## v0.6.1 (2025-05-19)

### Bug Fixes

- **engine**: Ensure aggregate checks run on original input DataFrame
  ([#35](https://github.com/sparkdq-community/sparkdq/pull/35),
  [`4dbd7a5`](https://github.com/sparkdq-community/sparkdq/commit/4dbd7a56e9c8cdf265f86b77b61ce6408ff1afe8))


## v0.6.0 (2025-05-17)

### Documentation

- **README**: Add spark installation guideline link
  ([`7c1049c`](https://github.com/sparkdq-community/sparkdq/commit/7c1049ccd53c0a49d00cbcf4e03ffac45685446d))

- **sphinx**: Add example link
  ([`c256caf`](https://github.com/sparkdq-community/sparkdq/commit/c256caf67992b837aca1200df95b26f141bb1c15))

### Features

- Add property to validation summary to indicate if all checks passed
  ([`b9c6e5b`](https://github.com/sparkdq-community/sparkdq/commit/b9c6e5b74ceb8c540d02d587e8f9bd6b548518ef))

- **check**: Add aggregate-level check to ensure completeness of specified columns
  ([`7ed6a3a`](https://github.com/sparkdq-community/sparkdq/commit/7ed6a3ab963d3b2a18d8264e80a9ee4585482814))

- **check**: Add CompletenessRatioCheck to validate non-null ratio of a column
  ([`5e32627`](https://github.com/sparkdq-community/sparkdq/commit/5e32627aca83327b9fe105c4b59d464397d360d9))

- **check**: Add DistinctRatioCheck to validate column cardinality
  ([`4c0c936`](https://github.com/sparkdq-community/sparkdq/commit/4c0c936981849684fb22e7f948bcac860a650d03))

- **check**: Add UniqueRatioCheck to validate minimum uniqueness ratio in a column
  ([`7a82ab3`](https://github.com/sparkdq-community/sparkdq/commit/7a82ab31b5791559e5cacfd28cc961efd8507a01))

- **check**: Add UniqueRowsCheck to detect duplicate rows based on full or partial column sets
  ([`71a7f1d`](https://github.com/sparkdq-community/sparkdq/commit/71a7f1dbfdc027728acf267bf1b3309e08087e5f))


## v0.5.2 (2025-05-13)

### Bug Fixes

- Replace reduce with aggregate
  ([`a04d586`](https://github.com/sparkdq-community/sparkdq/commit/a04d58659c920e1ee8ba2f892f7d34e87f9c00be))

### Documentation

- **examples**: Add simple example ([#30](https://github.com/sparkdq-community/sparkdq/pull/30),
  [`53fa344`](https://github.com/sparkdq-community/sparkdq/commit/53fa3446a4aa5d53567964f354de7df868b8b446))


## v0.5.1 (2025-05-12)

### Bug Fixes

- **check-definition**: Enforce kebab-case for check definition
  ([`4bd8ada`](https://github.com/sparkdq-community/sparkdq/commit/4bd8ada789fd6046963e359deda8e65b286bd351))

- **check-runner**: _dq_errors contains only validation failures
  ([`db80414`](https://github.com/sparkdq-community/sparkdq/commit/db80414f511e505486fdaf2b9850ae2455e195f8))

- **validation-summary**: Add string representation of validation summary
  ([`42657ac`](https://github.com/sparkdq-community/sparkdq/commit/42657ac1474635c9d947edf3567b3d5cd17f7af8))

### Documentation

- **sphinx**: Add correct import to example
  ([`f877df9`](https://github.com/sparkdq-community/sparkdq/commit/f877df99cd6e5dba5c242ea9742d126a7e655c07))

- **sphinx**: Add image to section Integration Patterns
  ([`0ce8424`](https://github.com/sparkdq-community/sparkdq/commit/0ce84248a93faf35362f996b0ea9153aa8d8751d))

- **sphinx**: Renamed filename of regex-match-check
  ([`1395b09`](https://github.com/sparkdq-community/sparkdq/commit/1395b09fc5a2f22fa401d92fd2e5a6647948b321))

- **sphinx**: Restructured documentation
  ([`2c9e670`](https://github.com/sparkdq-community/sparkdq/commit/2c9e670555f1e3f2370e32fc7be7b40316f931fa))


## v0.5.0 (2025-05-11)

### Documentation

- Update documentation and docstrings ([#12](https://github.com/sparkdq-community/sparkdq/pull/12),
  [`3decedc`](https://github.com/sparkdq-community/sparkdq/commit/3decedc4747d4c07a8c75b4aad480aa5eeed97da))

* docs(README): add pypi badge

* docs(checks): update explanation of inclusive parameter

* docs(api): add api to documentation and update docstrings

* chore(depenedencies): execute uv sync

### Features

- **check**: Add column-less-than-check
  ([`272cc2e`](https://github.com/sparkdq-community/sparkdq/commit/272cc2e432bb913e37358b938c77646e7c8b49a6))

- **check**: Add regex_match_check
  ([`2ac9252`](https://github.com/sparkdq-community/sparkdq/commit/2ac9252b33e557b7128a3861a4554bbebdfd418f))

- **check**: Add string-length-between-check
  ([`c73763b`](https://github.com/sparkdq-community/sparkdq/commit/c73763b4e122884d7c8fb5ca93fc7a0a8b2cc481))

- **check**: Add string-max-length-check
  ([`705ec53`](https://github.com/sparkdq-community/sparkdq/commit/705ec53a955632c76ca383aecdfe6da08c8ccd9b))

- **check**: Add string-min-length-check
  ([`59257d3`](https://github.com/sparkdq-community/sparkdq/commit/59257d3a8b6c20434fcb3e4ca33fb347cd00df8f))

- **checkset**: Add fluent api for check definition
  ([`691cf42`](https://github.com/sparkdq-community/sparkdq/commit/691cf423e87b2e31fdfc1a291b429d010a81261f))

- **versioning**: Add version in __init__.py
  ([`f8f8e6d`](https://github.com/sparkdq-community/sparkdq/commit/f8f8e6d8fbc4c2a8d5b4132a9803c574050bf844))

### Refactoring

- **string-min-length-check**: Simplify validate logic
  ([`b888d46`](https://github.com/sparkdq-community/sparkdq/commit/b888d4628348db93f7677b75f851889b04a58d31))


## v0.4.0 (2025-05-01)

### Features

- **checks**: Add inclusive parameter to range checks
  ([#11](https://github.com/sparkdq-community/sparkdq/pull/11),
  [`38c9415`](https://github.com/sparkdq-community/sparkdq/commit/38c94151c5fae031332ed036ae1f14ae8d9d7b26))


## v0.3.0 (2025-05-01)

### Documentation

- **custom-checks**: Add imports to examples
  ([`21e6d90`](https://github.com/sparkdq-community/sparkdq/commit/21e6d90752a2b5fe31e13f0045d38458bfa7545d))

- **custom-checks**: Add tutorial for custom checks
  ([`00281ff`](https://github.com/sparkdq-community/sparkdq/commit/00281ff597157672caa179ac3011cc51749dbb47))

### Features

- **check**: Add exactly-one-not-null-check
  ([`75cf32f`](https://github.com/sparkdq-community/sparkdq/commit/75cf32f7ce2980973a3d1d508000755eead2349f))

- **registry**: Add load_config_module to load custom checks dynamically
  ([`734474e`](https://github.com/sparkdq-community/sparkdq/commit/734474e5e47a0533a8492dadc7effc25e455b0b3))

### Refactoring

- Renamed subpackage factory to plugin
  ([`a580d7b`](https://github.com/sparkdq-community/sparkdq/commit/a580d7b79ca17737a0770fd3ba2076a74f6694fb))


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
