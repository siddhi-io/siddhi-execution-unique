siddhi-execution-unique
======================================

The **siddhi-execution-unique extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that processes event streams based on unique events.
Different types of unique windows are available to hold unique events based on the given unique key parameter.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-unique">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-unique/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-unique/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26">4.0.26</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-unique/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.execution.unique</groupId>
        <artifactId>siddhi-execution-unique</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-unique/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-unique/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#ever-window">ever</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This is a  window that is updated with the latest events based on a unique key parameter. When a new event that arrives, has the same value for the unique key parameter as an existing event, the existing event expires, and it is replaced by the later event.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#externaltimebatch-window">externalTimeBatch</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This is a batch (tumbling) time window that is determined based on external time (i.e., time stamps specified via an attribute in the events). It holds the latest unique events that arrived during the last window time period. The unique events are determined based on the value for a specified unique key parameter. When a new event arrives within the time window with a value for the unique key parameter that is the same as that of an existing event in the window, the existing event expires and it is replaced by the later event.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#first-window">first</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This is a window that holds only the first unique events that are unique according to the unique key parameter. When a new event arrives with a key that is already in the window, that event is not processed by the window.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#firstlengthbatch-window">firstLengthBatch</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This is a batch (tumbling) window that holds a specific number of unique events (depending on which events arrive first). The unique events are selected based on a specific parameter that is considered as the unique key. When a new event arrives with a value for the unique key parameter that matches the same of an existing event in the window, that event is not processed by the window.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#firsttimebatch-window">firstTimeBatch</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>A batch (tumbling) time window that holds first unique events according to the unique key parameter that have arrived during window time period and gets updated for each window time period. When a new event arrives with a key which is already in the window, that event is not processed by the window.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#length-window">length</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This is a sliding length window that holds the latest window length unique events according to the unique key parameter and gets updated for each event arrival and expiry. When a new event arrives with the key that is already there in the window, then the previous event is expired and new event is kept within the window.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#lengthbatch-window">lengthBatch</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This is a batch (tumbling) window that holds a specified number of latest unique events. The unique events are determined based on the value for a specified unique key parameter. The window is updated for every window length (i.e., for the last set of events of the specified number in a tumbling manner). When a new event that arrives within the a window length has the same value for the unique key parameter as an existing event is the window, the previous event is replaced by the new event.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#time-window">time</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This is a sliding time window that holds the latest unique events that arrived during the last window time period. The unique events are determined based on the value for a specified unique key parameter. The window is updated with each event arrival and expiry. When a new event that arrives within a window time period has the same value for the unique key parameter as an existing event in the window, the previous event is replaced by the later event.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#timebatch-window">timeBatch</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This is a batch (tumbling) time window that is updated with the latest events based on a unique key parameter. If a new event that arrives within the window time period has a value for the key parameter which matches that of an existing event, the existing event expires and it is replaced by the later event. </p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-unique/api/4.0.26/#timelengthbatch-window">timeLengthBatch</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>*<br><div style="padding-left: 1em;"><p>This is a batch (tumbling) time length window that is updated with the latest events based on a unique key parameter. The window will tumble upon elapse of the time window or when length number of unique events have arrived. If a new event that arrives within the window period has a value for the key parameter which matches that of an existing event, the existing event expires and it is replaced by the later event. </p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-unique/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-unique/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
