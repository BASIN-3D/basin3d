from basin3d.core.plugin import DataSourcePluginPoint


class BetaSourcePlugin(DataSourcePluginPoint):
    class DataSourceMeta:
        location = 'https://asource.foo/'
        id = 'Beta'  # unique id for the datasource
        id_prefix = 'B'
        name = id  # Human Friendly Data Source Name
