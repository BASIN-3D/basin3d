from basin3d.core.plugin import DataSourcePluginPoint


class ComplexmapSourcePlugin(DataSourcePluginPoint):
    class DataSourceMeta:
        location = 'https://asource.foo/'
        id = 'Complexmap'  # unique id for the datasource
        id_prefix = 'C'
        name = id  # Human Friendly Data Source Name
