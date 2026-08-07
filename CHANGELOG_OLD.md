# Older changes
## 2.1.8 (2022-08-13)
* (riversource/Apollon77) Optimize getHistory query by using "UNION ALL"
* (Apollon77) Fix crash cases reported by Sentry

## 2.1.7 (2022-06-30)
* (Apollon77) Fix crash cases reported by Sentry

## 2.1.6 (2022-06-27)
* (Apollon77) Allowed removing a configuration value for "round" in config again

## 2.1.5 (2022-06-27)
* (Apollon77) When no count is provided for aggregate "none" or "onchange" then the limit (default 2000) is used as count to define the number of data to return.
* (Apollon77) Fix the initialization of types and IDs for some cases.

## 2.1.3 (2022-06-12)
* (Apollon77) Make sure the debug log is active, according to the settings

## 2.1.2 (2022-06-08)
* (Apollon77) Huge performance optimizations for GetHistory calls

## 2.1.1 (2022-05-30)
* (Apollon77) Fix crash cases reported by Sentry

## 2.1.0 (2022-05-27)
* (Apollon77) Fix crash cases reported by Sentry
* (Apollon77) Fix several places where pooled connections might have not been returned to pool correctly and add logging for it
* (Apollon77) Work around an issue in used Pooling library that potentially gave out too many connections
* (Apollon77) Optimize retention check to better spread the first checks over time
* (Apollon77) Default to not use datapoint buffering as in 1.x when set to 0
* (Apollon77) Make sure disabling "Log changes only" also really does not log the changes anymore
* (Apollon77) Allow storeState and GetHistory also to be called for "unknown ids"
* (Apollon77) Adjust the fallback logic for type detection to use the type of the state value to log as last fallback
* (Apollon77) Fix storing booleans on MSSQL

## 2.0.2 (2022-05-11)
* (Apollon77) BREAKING: Configuration is only working in the new Admin 5 UI!
* (Apollon77) Did bigger adjustments to the recording logic and added a lot of new Features. Please refer to Changelog and Forum post for details.

## 2.0.0 (2022-05-11)
* (Apollon77) Breaking: Configuration is only working in the new Admin 5 UI!
* (Apollon77) Breaking! Did bigger adjustments to the recording logic. Debounce is refined and blockTime is added to differentiate between the two checks
* (Apollon77) Breaking! GetHistory requests now need to deliver the ts in milliseconds! Make sure to use up-to-date scripts and Charting UIs
* (Apollon77) Add RAM buffering and mass inserts for logging
* (Apollon77) New setting added to disable the "logging of additional values for charting optimization" - then only the expected data are logged
* (Apollon77) Add flag returnNewestEntries for GetHistory to determine which records to return when more entries as "count" are existing for aggregate "none"
* (Apollon77) Add support for addId getHistory flag for GetHistory
* (Apollon77) Add new Debug flag to enable/disable debug logging on datapoint level (default is false) to optimize performance
* (Apollon77) Add aggregate method "percentile" to calculate the percentile (0..100) of the values (requires `options.percentile` with the percentile level, defaults to 50 if not provided). Basically the same as Quantile, just different levels are used
* (Apollon77) Add aggregate method "quantile" to calculate the quantile (0..1) of the values (requires `options.quantile` with the quantile level, defaults to 0.5 if not provided). Basically the same as Percentile just different levels are used
* (Apollon77) Add (experimental) method "integral" to calculate the integral of the values. Requires options.integralUnit with the time duration of the integral in seconds, defaults to 60s if not provided. Optionally, a linear interpolation can be done by setting options.integralInterpolation to "linear"
* (Apollon77) When request contains flag removeBorderValues: true, the result then cut the additional pre- and post-border values out of the results
* (Apollon77) Enhance the former "Ignore below 0" feature and now allow specifying to ignore below or above specified values. The old setting is converted to the new one
* (Apollon77) Upgrade MSSQL and MySQL drivers incl. Support for MySQL 8
* (Apollon77) Make sure that min change delta allows numbers entered with comma (german notation) in all cases
* (Apollon77) Add support to specify how to round numbers on query per datapoint
* (Apollon77) Do not log passwords for Postgres connections
* (Apollon77) Optimize SSL support for database connections including option to allow self-signed certificates
* (Apollon77) Allows to specify custom retention duration in days
* (winnyschuster) Fix Insert statement for MSSQL ts_counter
* (winnyschuster) type of ts in user queries corrected

## 1.16.2 (2022-02-16)
* (bluefox) Marked interpolated data with `i=true`

## 1.16.1 (2021-12-19)
* (Excodibur) Hide settings not relevant when "log changes only" is not used
* (Apollon77) Allow all number values for debounce again

## 1.16.0 (2021-12-14)
* (bluefox) Support only `js-controller` >= 3.3.x
* (bluefox) Used system/custom view for collecting the objects
* (bluefox) Implemented option to ignore zero- or/and below zero- values

## 1.15.7 (2021-04-28)
* (bluefox) fixed the support of Admin5

## 1.15.6 (2021-04-19)
* (bluefox) added support of Admin5

## 1.15.5 (2021-01-22)
* (Apollon77) make sure message query is a string (Sentry)

## 1.15.4 (2021-01-17)
* (Apollon77) Optimize stop handling

## 1.15.3 (2020-08-29)
* (bluefox) Added the option "Do not create database". E.g. if DB was created and it does not required to do that, because the user does not have enough rights.

## 1.15.2 (2020-07-26)
* (Apollon77) prevent wrong errors that realId is missing

## 1.15.1 (2020-07-20)
* (Apollon77) implement a workaround for postgres problem

## 1.15.0 (2020-07-19)
*BREAKING* This version only accepts Node.js 10.x+ (because sqlite3 was upgraded)
* (Apollon77) Prevent crash case (Sentry IOBROKER-SQL-16, IOBROKER-SQL-15, IOBROKER-SQL-1K)

## 1.14.2 (2020-06-23)
* (bluefox) Fixed error for data storage

## 1.14.1 (2020-06-17)
* (bluefox) Corrected error for objects with mixed type

## 1.14.0 (2020-05-20)
* (bluefox) added the range deletion and the delete all operations

## 1.13.1 (2020-05-20)
* (bluefox) added changed and delete operations

## 1.12.6 (2020-05-08)
* (bluefox) set default history if not yet set

## 1.12.5 (2020-05-05)
* (Apollon77) Crash prevented for invalid objects (Sentry IOBROKER-SQL-X)

## 1.12.4 (2020-05-04)
* (Apollon77) Potential crash fixed when disabling data points too fast (Sentry IOBROKER-SQL-W) 
* (Apollon77) Always set "encrypt" flag, even if false because else might en in default true (see https://github.com/tediousjs/tedious/issues/931)

## 1.12.3 (2020-04-30)
* (Apollon77) Try to create indexes on MSSQL to speed up things. Infos are shown if not possible to be able for the user to do it themself. Timeout is 15s

## 1.12.2 (2020-04-30)
* (Apollon77) MSSQL works again

## 1.12.1 (2020-04-26)
* (Apollon77) Fix potential crash (Sentry)

## 1.12.0 (2020-04-23)
* (Apollon77) Implement max Connections setting and respect it, now allows to control how many concurrent connections to database are used (default 100) and others wait up to 10s for a free connection before failing)
* (Apollon77) Change dependencies to admin to a global dependency
* (Apollon77) Update connection status also in between
* (Apollon77) fix some potential crash cases (Sentry reported)
* (Omega236) Add id to error message for queries
* (Apollon77) update pg to stay compatible with nodejs 14
* (Apollon77) Start clearly ending timeouts on unload ... still some cases left!

## 1.11.1 (2020-04-19)
* __Requires js-controller >= 2.0.0__
* (Apollon77) removed usage of adapter.objects
* (Apollon77) check if objects have changed and ignore unchanged
* (Apollon77) Add Sentry for Error Reporting with js-controller 3.0
* (Apollon77) Make sure value undefined is ignored

## 1.10.1 (2020-04-12)
* (bluefox) Converted to ES6
* (bluefox) The counter functionality was implemented.

## 1.9.5 (2019-05-15)
* (Apollon77) Add support for nodejs 12

## 1.9.4 (2019-02-24)
* (Apollon77) Fix several smaller issues and topics
* (Apollon77) Optimize Texts (for Admin v3 UI)

## 1.9.0 (2018-06-19)
* (Apollon77) Add option to log datapoints as other ID (alias) to easier migrate devices and such

## 1.8.0 (2018-04-29)
* (Apollon77) Update sqlite3, nodejs 10 compatible
* (BuZZy1337) Admin fix

## 1.7.4 (2018-04-15)
* (Apollon77) Fix getHistory

## 1.7.3 (2018-03-28)
* (Apollon77) Respect 'keep forever' setting for retention from data point configuration

## 1.7.2 (2018-03-24)
* (Apollon77) Disable to write NULLs for SQLite

## 1.7.1 (2018-02-10)
* (Apollon77) Make option to write NULL values on start/stop boundaries configurable

## 1.6.9 (2018-02-07)
* (bondrogeen) Admin3 Fixes
* (Apollon77) optimize relog feature and other things

## 1.6.7 (2018-01-31)
* (Bluefox) Admin3 Fixes
* (Apollon77) Relog and null log fixes

## 1.6.2 (2018-01-30)
* (Apollon77) Admin3 Fixes

## 1.6.0 (2018-01-14)
* (bluefox) Ready for Admin3

## 1.5.8 (2017-10-05)
* (Apollon77) fix relog value feature

## 1.5.7 (2017-08-10)
* (bluefox) add "save last value" option

## 1.5.6 (2017-08-02)
* (Apollon77) fix behaviour of log interval to always log the current value

## 1.5.4 (2017-06-12)
* (Apollon77) fix dependency to other library

## 1.5.3 (2017-04-07)
* (Apollon77) fix in datatype conversions

## 1.5.0 (2017-03-02)
* (Apollon77) Add option to define storage datatype per datapoint inclusing converting the value if needed

## 1.4.6 (2017-02-25)
* (Apollon77) Fix typo with PostgrSQL

## 1.4.5 (2017-02-18)
* (Apollon77) Small fix again for older configurations
* (Apollon77) fix for DBConverter Analyze function

## 1.4.3 (2017-02-11)
* (Apollon77) Small fix for older configurations

## 1.4.2 (2017-01-16)
* (bluefox) Fix handling of float values in Adapter config and Datapoint config.

## 1.4.1
* (Apollon77) Rollback to sql-client 0.7 to get rid of the mmagic dependecy that brings problems on older systems

## 1.4.0 (2016-12-02)
* (Apollon77) Add messages enableHistory/disableHistory
* (Apollon77) add support to log changes only if the value differs from a minimum value for numbers

## 1.3.4 (2016-11)
* (Apollon77) Allow database names with '-' for MySQL

## 1.3.3 (2016-11)
* (Apollon77) Update dependencies

## 1.3.2 (2016-11-21)
* (bluefox) Fix insert of string with '

## 1.3.0 (2016-10-29)
* (Apollon77) Added an option to re-log unchanged values to make it easier for visualization

## 1.2.1 (2016-08-30)
* (bluefox) Fix selector for SQL objects

## 1.2.0 (2016-08-30)
* (bluefox) compatible only with new admin

## 1.0.10 (2016-08-27)
* (bluefox) change name of object from "history" to "custom"

## 1.0.10 (2016-07-31)
* (bluefox) fix multi requests if sqlite

## 1.0.9 (2016-06-14)
* (bluefox) allow settings for parallel requests

## 1.0.7 (2016-05-31)
* (bluefox) Draw a line to the end if ignore null

## 1.0.6 (2016-05-30)
* (bluefox) allow setup DB name for mysql and mssql

## 1.0.5 (2016-05-29)
* (bluefox) switch max and min with each other

## 1.0.4 (2016-05-29)
* (bluefox) check retention of data if set "never"

## 1.0.3 (2016-05-28)
* (bluefox) try to calculate old timestamps

## 1.0.2 (2016-05-24)
* (bluefox) fix error with io-package

## 1.0.1 (2016-05-24)
* (bluefox) fix error with SQLite

## 1.0.0 (2016-05-20)
* (bluefox) change default aggregation name

## 0.3.3 (2016-05-18)
* (bluefox) fix postgres

## 0.3.2 (2016-05-13)
* (bluefox) queue select if IDs and FROMs queries for sqlite

## 0.3.1 (2016-05-12)
* (bluefox) queue delete queries too for sqlite

## 0.3.0 (2016-05-08)
* (bluefox) support of custom queries
* (bluefox) only one request simultaneously for sqlite
* (bluefox) add tests (primitive and only sql)

## 0.2.0 (2016-04-30)
* (bluefox) support of milliseconds
* (bluefox) fix sqlite

## 0.1.4 (2016-04-25)
* (bluefox) fix deletion of old entries

## 0.1.3 (2016-03-08)
* (bluefox) do not print errors twice

## 0.1.2 (2015-12-22)
* (bluefox) fix MS-SQL port settings

## 0.1.1 (2015-12-19)
* (bluefox) fix error with double entries

## 0.1.0 (2015-12-14)
* (bluefox) support of strings

## 0.0.3 (2015-12-06)
* (smiling_Jack) Add demo Data ( todo: faster insert to db )
* (smiling_Jack) change aggregation (now same as history Adapter)
* (bluefox) bug fixing

## 0.0.2 (2015-12-06)
* (bluefox) allow only 1 client for SQLite

## 0.0.1 (2015-11-19)
* (bluefox) initial commit
