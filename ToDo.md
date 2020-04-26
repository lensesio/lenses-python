# ToDo

List of objectives for lensesio pylib

## Bugs

Subscribing creates a continuous query but does not list the client as a subscriber. This blocks any auto-commits

## Update

**Subscribe method should support Queues**
- After calling subscribe method, we should be able to consume a queue rather than
Adding working with functions inside the subscribe loop

**Dynamic krb_auth script distribution**
Currently krb auth module can be installed by passing [kerberos] duing the pip install.
This however does not exclude the distribution of krb_auth scripts. Also, the main process
is aware of it but blocks the execution.
The goals are:
- Do not distribute the krb_auth scripts
- Distributed main process should not have any krb_auth case

## Features

**Custom Auth Class**
Allow custom authentication by executing a user defined function
The goals are:
- Able to pass a custom function that returns a single token back to the main process

**AD Auth**
Support dynamic Microsoft AD auth as we do with kerberos auth
The goals are:
- Include dynamic module installation into setup.py
- Distribute the ad_auth scripts based on the installation
- Exclude the ad_auth code in the main process if the script has not been distributed

**Describe Flows**
Describe flows will further filter get_flows() method and create a dict view with a tree of all descendant nodes
from root (first source connector) to sink (last source connector in the pipe.)

For example:

    SourceA -> topicA -> SQL Proc1 -> topicB -> SinkA
                                        |
    SourceB  ---------------------------| -> SQL Proc2 -> topicC -> SinkB
    
In the above example, there are two data pipes.
1) A `Source` Connnector (**SourceA**) writes data into `Topic` (**topicA**). The `Topic` is processed by
a `SQL Processors` (**SQL Proc1**) which dumps the products into a `Topic` (**topicB**). Last the `Topic` **topicB** is
pushed into an external data bank via a `Sink` Connector (**SinkA**)

2) A `Source` Connector (**SourceB**) writes data into `Topic` (**topicB**) which is processed by a `SQL Processors` (**SQL Proc1**) which in turn dumps the products into a `Topic` (**topicC**). Last the **topicC** is pushed into an external data bank via **SinkB**.
