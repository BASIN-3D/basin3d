from basin3d.core.plugin import DataSourcePluginPoint, basin3d_plugin


@basin3d_plugin
class IncompleteMetaSourcePlugin(DataSourcePluginPoint):
    class DataSourceMeta:
        pass
