<h1>Configuration via Options or File</h1>

ToroDB Stampede would start with custom configuration options. There are two ways to do it, using command modifiers or using a configuration file. The recommended way is using a configuration file because it is more versatile and self-documented.

Use the `-c` parameter to specify a configuration file.

```no-highlight
torodb-stampede -c myconfiguration.yml

```
To print the effective configuration in YAML format, you can use the `-l` parameter. If you don't specify a configuration file, the effective defaults are shown:

```no-highlight
torodb-stampede -l
```
You can write the default configuration into a file and adjust it as needed:

!!! warning
    Don't overwrite your existing configuration.

```no-highlight
torodb-stampede -l > myconfiguration.yml
```
