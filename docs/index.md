Siddhi Execution Unique
======================================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-unique/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-unique/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-execution-unique.svg)](https://github.com/siddhi-io/siddhi-execution-unique/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-execution-unique.svg)](https://github.com/siddhi-io/siddhi-execution-unique/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-execution-unique.svg)](https://github.com/siddhi-io/siddhi-execution-unique/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-execution-unique.svg)](https://github.com/siddhi-io/siddhi-execution-unique/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-execution-unique extension** is a <a target="_blank" href="https://siddhi.io/">Siddhi</a> extension that retains and process unique events based on the given parameters.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 5.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.execution.unique/siddhi-execution-unique/">here</a>.
* Versions 4.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.execution.unique/siddhi-execution-unique">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4">5.0.4</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#deduplicate-stream-processor">deduplicate</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">Removes duplicate events based on the <code>unique.key</code> parameter that arrive within the <code>time.interval</code> gap from one another.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#ever-window">ever</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">Window that retains the latest events based on a given unique keys. When a new event arrives with the same key it replaces the one that exist in the window.&lt;b&gt;This function is not recommended to be used when the maximum number of unique attributes are undefined, as there is a risk of system going out to memory&lt;/b&gt;.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#externaltimebatch-window">externalTimeBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This is a batch (tumbling) time window that is determined based on an external time, i.e., time stamps that are specified via an attribute in the events. It holds the latest unique events that arrived during the last window time period. The unique events are determined based on the value for a specified unique key parameter. When a new event arrives within the time window with a value for the unique key parameter that is the same as that of an existing event in the window, the existing event expires and it is replaced by the new event.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#first-window">first</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This is a window that holds only the first set of unique events according to the unique key parameter. When a new event arrives with a key that is already in the window, that event is not processed by the window.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#firstlengthbatch-window">firstLengthBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This is a batch (tumbling) window that holds a specific number of unique events (depending on which events arrive first). The unique events are selected based on a specific parameter that is considered as the unique key. When a new event arrives with a value for the unique key parameter that matches the same of an existing event in the window, that event is not processed by the window.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#firsttimebatch-window">firstTimeBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">A batch-time or tumbling window that holds the unique events according to the unique key parameters that have arrived within the time period of that window and gets updated for each such time window. When a new event arrives with a key which is already in the window, that event is not processed by the window.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#length-window">length</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This is a sliding length window that holds the events of the latest window length with the unique key and gets updated for the expiry and arrival of each event. When a new event arrives with the key that is already there in the window, then the previous event expires and new event is kept within the window.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#lengthbatch-window">lengthBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This is a batch (tumbling) window that holds a specified number of latest unique events. The unique events are determined based on the value for a specified unique key parameter. The window is updated for every window length, i.e., for the last set of events of the specified number in a tumbling manner. When a new event arrives within the window length having the same value for the unique key parameter as an existing event in the window, the previous event is replaced by the new event.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#time-window">time</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This is a sliding time window that holds the latest unique events that arrived during the previous time window. The unique events are determined based on the value for a specified unique key parameter. The window is updated with the arrival and expiry of each event. When a new event that arrives within a window time period has the same value for the unique key parameter as an existing event in the window, the previous event is replaced by the new event.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#timebatch-window">timeBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This is a batch (tumbling) time window that is updated with the latest events based on a unique key parameter. If a new event that arrives within the time period of a windowhas a value for the key parameter which matches that of an existing event, the existing event expires and it is replaced by the latest event. </p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-unique/api/5.0.4/#timelengthbatch-window">timeLengthBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This is a batch or tumbling time length window that is updated with the latest events based on a unique key parameter. The window tumbles upon the elapse of the time window, or when a number of unique events have arrived. If a new event that arrives within the period of the window has a value for the key parameter which matches the value of an existing event, the existing event expires and it is replaced by the new event. </p></p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-unique/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.
