<h1>Configuration via Options or File</h1>

ToroDB Stampede would start with custom configuration options. There are two ways to do it, using command modifiers or using a configuration file. The recommended way is using a configuration file because it is more versatile and self-documented.

To use the configuration file, the `-c` parameter should be specified.

```no-highlight
torodb-stampede -c myconfiguration.yml

```

Also you can check configuration used by ToroDB Stampede using the `-l` parameter.

```no-highlight
torodb-stampede -l
```

The previous sections talk about basic configuration of the system, but it is highly probable that some specific configuration must be done to work in production environments.
