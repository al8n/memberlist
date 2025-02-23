# ToyDB

A toy eventually consensus distributed database.

## Installation

```bash
cargo install --path .
```

## Run

- In the first terminal

  ```bash
  toydb start --id instance1 --addr 127.0.0.1:7001 --meta instance1 --rpc-addr toydb.instance1.sock
  ```

- In the second terminal

  - Start instance 2

    ```bash
    toydb start --id instance2 --addr 127.0.0.1:7002 --meta instance2 --rpc-addr toydb.instance2.sock
    ```
  
  - Send the join command to instance2 and let it join to instance1
  
    ```bash
    toydb join --id instance1 --addr 127.0.0.1:7001 --rpc-addr toydb.instance2.sock
    ```

- In the third terminal

  - Start instance 3

    ```bash
    toydb start --id instance3 --addr 127.0.0.1:7003 --meta instance3 --rpc-addr toydb.instance3.sock
    ```
  
  - Send the join command to instance3 and let it join to instance1 (can also join to instance 2)
  
    ```bash
    toydb join --id instance1 --addr 127.0.0.1:7001 --rpc-addr toydb.instance3.sock
    ```

- In the fourth terminal

  - Insert a key - value to the instance1

    ```bash
    toydb set --key foo --value bar --rpc-addr toydb.instance1.sock
    ```
  
  - After some seconds, you can get the value from any one of three instances

    ```bash
    toydb get --key foo --rpc-addr toydb.instance1.sock
    ```

    ```bash
    toydb get --key foo --rpc-addr toydb.instance2.sock
    ```

    ```bash
    toydb get --key foo --rpc-addr toydb.instance3.sock
    ```
