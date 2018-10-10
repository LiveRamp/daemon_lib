# daemon_lib
daemon_lib is a Java library that makes it easy to write parallelized task processors. The primary motivation is to make it easy to orchestrate multiple independent instances of a single program operating on different inputs. The core library has no infrastructure dependencies beyond access to a working directory on disk.

## Adding the dependency

In Maven, make your project section look like this:

```
<project>
<!-- All the other stuff -->

  <dependencies>
    <!-- All your other dependencies -->
    <dependency>
      <groupId>com.liveramp</groupId>
      <artifactId>daemon_lib</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>

  <repositories>
    <repository>
      <id>liveramp-repositories</id>
      <name>Liveramp Repositories</name>
      <url>http://repository.liveramp.com/artifactory/liveramp-repositories</url>
      <snapshots>
        <enabled>true</enabled>
        <updatePolicy>always</updatePolicy>
      </snapshots>
    </repository>
  </repositories>
</project>
```

The repository section is necessary because this project hasn't been published to Maven Central yet.

## Background
We often find ourselves building systems that are essentially many instances of a single “workflow” operating on different inputs, commonly as the backend for an asynchronous service. These instances run in parallel, either as threads within the main application process for tasks that are short-lived, or as separate background processes on the same machine for tasks that need to survive application restarts. 


daemon_lib handles all the boilerplate involved with building such a system, exposing control through a combination of configuration parameters and injectable callbacks to handle life cycle events. 


## Primary Constructs
* Daemon: A highly-injectable, long-running process that delegates to user-provided classes to fetch new inputs, kick off workflows based on these inputs, and handle life cycle events. It is the runtime entry-point to the framework.

### User-defined:
* Joblet: A script that performs a single unit of work. It acts on a JobletConfig.
* JobletConfig: A serializable object that encapsulates the inputs to a Joblet.
* JobletConfigProducer: A class that returns the next unit of work. It is invoked by the main Daemon process when it is ready to do more work.
* Callbacks: All callbacks receive the relevant JobletConfig as an argument.
    * onNewConfig: Fired just before a config is executed. Primarily used to update state so that the same config is not returned the next time the JobletConfigProducer is invoked.
    * onSuccess: Fired when the joblet terminates successfully. The definition of success depends on the JobletExecutor.
    * onFailure: Fired when the onSuccess is not.

### User-configured:

* JobletExecutor: Defines *how* a joblet should be executed. The core library ships with a couple of implementations:
    * Threading: Joblets are run within Callables submitted to a fixed size thread pool.
    * Forking: Forks a background process for each Joblet. It handles tracking this process and calling the relevant callbacks when the Joblet is done. It stores state on disk and is able to track a Joblet even after the main daemon process is restarted.
