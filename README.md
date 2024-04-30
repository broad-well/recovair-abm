# recovair-abm

**recovair-abm:** Agent-based airline network model for disruption recovery decision-making

## Installing recovair-abm

Installing recovair-abm requires a [supported version of Node and Rust](https://github.com/neon-bindings/neon#platform-support).

You can install the project with npm. In the project directory, run:

```sh
$ npm install
```

This fully installs the project, including installing any dependencies and running the build.

## Building recovair-abm

If you have already installed the project and only want to run the build, run:

```sh
$ npm run build
```

This command uses the [cargo-cp-artifact](https://github.com/neon-bindings/cargo-cp-artifact) utility to run the Rust build and copy the built library into `./index.node`.

## Available Scripts

In the project directory, you can run:

### `npm install`

Installs the project, including running `npm run build`.

### `npm build`

Builds the Node addon (`index.node`) from source.

Additional [`cargo build`](https://doc.rust-lang.org/cargo/commands/cargo-build.html) arguments may be passed to `npm build` and `npm build-*` commands. For example, to enable a [cargo feature](https://doc.rust-lang.org/cargo/reference/features.html):

```
npm run build -- --feature=beetle
```

#### `npm build-debug`

Alias for `npm build`.

#### `npm build-release`

Same as [`npm build`](#npm-build) but, builds the module with the [`release`](https://doc.rust-lang.org/cargo/reference/profiles.html#release) profile. Release builds will compile slower, but run faster.

### `npm test`

Runs the unit tests by calling `cargo test`. You can learn more about [adding tests to your Rust code](https://doc.rust-lang.org/book/ch11-01-writing-tests.html) from the [Rust book](https://doc.rust-lang.org/book/).

## Getting RecovAir to run

1. You need to generate necessary scenario data. Gather the following files and place them into a new folder at `db/truth`:
    - `ACFTREF.txt` (Aircraft Reference File from the [FAA](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download))
    - `MASTER.txt` (Aircraft Registration Master File from the [FAA](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download))
    - `T_MASTER_CORD.csv` (Aviation Support Tables: Master Coordinate from the [BTS](https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FLL&QO_fu146_anzr=N8vn6v10%20f722146%20gnoyr5))
    - `T_ONTIME_REPORTING.csv` (On-Time: Reporting Carrier On-Time Performance from the [BTS](https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr))
    - `T_T100D_MARKET_US_CARRIER_ONLY.csv` (T-100 Domestic Market from the [BTS](https://www.transtats.bts.gov/DL_SelectFields.asp?gnoyr_VQ=FIL&QO_fu146_anzr=Nv4%20Pn44vr45))
    - `T_T100D_SEGMENT_US_CARRIER_ONLY.csv` (T-100 Domestic Segment from the [BTS](https://www.transtats.bts.gov/DL_SelectFields.asp?gnoyr_VQ=FIM&QO_fu146_anzr=Nv4%20Pn44vr45))
2. Initialize a test database at `db/generator/test.db`. In `db/generator`, run `sqlite3 test.db < ../sqlite_schema.sql`.
3. Run `db/generator/preprocess.py` to seed the test database. If necessary, create a virtual environment and install dependencies from `db/generator/requirements.txt`. It will take a moment to generate synthetic crew information and passenger demand.
4. Run `npm run build-release` to compile the Rust part of the model.
5. Run `node test_jan28.js` and verify that it prints the keys of the object representing the final state of the simulation. The object itself is enormous, so to relieve the terminal, we don't print it out by default.