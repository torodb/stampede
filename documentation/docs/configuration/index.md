<h1>Configuration via Options or File</h1>

There are two ways to provide custom configuration to ToroDB Stampede on startup:

- Using command line options
- Using a configuration file

We generally recommend using a configuration file because it is more versatile and self-documented.

To specify the location of the configuration file, use the `-c` parameter:

```no-highlight
torodb-stampede -c myconfiguration.yml

```
To print the effective configuration in YAML format, you can use the `-l` parameter. If you don't specify a configuration file, the effective defaults are shown:

```no-highlight
torodb-stampede -l
```
You can write the default configuration into a file and adjust it as needed:

!!! warning
    Make sure you don't owerwrite your existing configuration.

```no-highlight
torodb-stampede -l > myconfiguration.yml
```
