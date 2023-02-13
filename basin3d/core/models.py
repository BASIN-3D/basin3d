"""

.. currentmodule:: basin3d.core.models

:synopsis: The BASIN-3D  Models
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top

"""

import datetime
import enum
import json

from collections import namedtuple
from dataclasses import dataclass, field
from itertools import repeat
from numbers import Number
from typing import List, Optional, Union

from basin3d.core import monitor
from basin3d.core.schema.enum import FeatureTypeEnum, FEATURE_SHAPE_TYPES, MappedAttributeEnum, MAPPING_DELIMITER, NO_MAPPING_TEXT
from basin3d.core.translate import get_datasource_mapped_attribute, translate_attributes
from basin3d.core.types import SpatialSamplingShapes

logger = monitor.get_logger(__name__)


class JSONSerializable:
    """
    Make a Data class serializable to json
    """

    def to_json(self):
        def props(o):
            """Convert object to dict.  If prefixed with, _ remove it"""
            try:
                map = {}
                for k in o.__dict__.keys():
                    if not k.startswith("__"):
                        if k.startswith("_"):
                            map[k[1:]] = o.__dict__[k]
                        else:
                            map[k] = o.__dict__[k]
                return map
            except Exception:
                # There is not __dict__, return a string representation
                return str(o)

        return json.dumps(self, default=props,
                          sort_keys=True, indent=4)

    def to_dict(self):
        return json.loads(self.to_json())


@dataclass
class DataSource(JSONSerializable):
    """
    Data Source definition

    Fields:
        - *id:* string (inherited)
        - *name:* string
        - *id_prefix:* string, prefix that is added to all data source ids
        - *location:*
        - *credentials:*

    """
    id: str = ''  # not sure we need this id??
    name: str = ''
    id_prefix: str = ''
    location: str = ''
    credentials: dict = field(default_factory=dict)

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return self.name


@dataclass
class ObservedProperty(JSONSerializable):
    """
    Defining the properties being observed (measured). See http://vocabulary.odm2.org/variablename/ for controlled vocabulary

    Fields:
        - *basin3d_vocab:* string,
        - *full_name:* string,
        - *categories:* List of strings (in order of priority).
        - *units:* string

    See http://vocabulary.odm2.org/variabletype/ for options, although I think we should have our own list (theirs is a bit funky).
    """
    basin3d_vocab: str = ''
    full_name: str = ''
    categories: list = field(default_factory=list)
    units: str = field(default='')

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return self.basin3d_vocab


@dataclass
class AttributeMapping(JSONSerializable):
    """
    A data class for attribute mappings between datasource vocabularies and BASIN-3D vocabularies.
    These are the associations defined in the datasource (i.e., plugin) mapping file.

    Fields:
         - *attr_type:* Attribute Type; e.g., STATISTIC, RESULT_QUALITY, OBSERVED_PROPERTY; separate compound mappings with ':'
         - *basin3d_vocab:* The BASIN-3D vocabulary; separate compound mappings with ':'
         - *basin3d_desc:* The BASIN-3D vocabulary descriptions; objects or enum
         - *datasource_vocab:* The datasource vocabulary
         - *datasource_desc:* The datasource vocabulary description
         - *datasource:* The datasource of the mapping
    """
    attr_type: str
    basin3d_vocab: str
    basin3d_desc: list
    datasource_vocab: str
    datasource_desc: str
    datasource: DataSource = DataSource()

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return self.datasource_vocab


@dataclass
class MappedAttribute(JSONSerializable):
    """
    A data class for an attribute that is translated (i.e., mapped) from a datasource vocabulary to BASIN-3D vocabulary.
    Note that this model holds an AttributeMapping that maybe compound in nature; however this class specifies only one attribute types.
    For example, if the AttributeMapping is for a compound mapping of attribute types OBSERVED_PROPERTY:SAMPLING_MEDIUM,
    then the attr_type field would be either OBSERVED_PROPERTY or SAMPLING_MEDIUM but not both.

    Fields:
         - *attr_type:* Attribute Type; e.g., STATISTIC, RESULT_QUALITY, OBSERVED_PROPERTY, etc; single type only
         - *attr_mapping:* AttributeMapping as described in the datasource's (i.e., plugin's mapping file).
    """
    attr_type: MappedAttributeEnum
    attr_mapping: AttributeMapping

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return self.get_basin3d_vocab()

    def get_basin3d_vocab(self) -> Optional[str]:
        if self.attr_mapping.basin3d_vocab == NO_MAPPING_TEXT:
            return NO_MAPPING_TEXT
        attrs = self.attr_mapping.attr_type.split(MAPPING_DELIMITER)
        b3d_vocabs = self.attr_mapping.basin3d_vocab.split(MAPPING_DELIMITER)
        try:
            for attr, vocab in zip(attrs, b3d_vocabs):
                if attr == self.attr_type.upper():
                    return vocab
        except Exception as e:
            logger.error(f'Issue returning basin3d_vocab for MappedAttribute. THIS SHOULD NEVER HAPPEN. {e}')
        return None

    def get_basin3d_desc(self):
        b3d_descs = self.attr_mapping.basin3d_desc
        if not b3d_descs:
            return None
        attrs = self.attr_mapping.attr_type.split(MAPPING_DELIMITER)
        try:
            for attr, desc in zip(attrs, b3d_descs):
                if attr == self.attr_type.upper():
                    return desc
        except Exception as e:
            logger.error(f'Issue returning basin3d_desc for MappedAttribute. THIS SHOULD NEVER HAPPEN. {e}')
        return None

    def get_datasource_vocab(self) -> str:
        return self.attr_mapping.datasource_vocab

    def get_datasource_desc(self) -> str:
        return self.attr_mapping.datasource_desc


class Base(JSONSerializable):
    """
    Base synthesis model class. All classes that extend this are immutable.
    """

    def __init__(self, plugin_access, **kwargs):

        self._datasource_ids = None
        self._datasource = plugin_access and plugin_access.datasource
        self._id = None
        self._original_id = None

        if not isinstance(kwargs, dict):
            raise TypeError("Expected a dict")

        # any ids listed should have the DataSource.id_prefix
        if 'datasource_ids' in kwargs.keys():
            for id in kwargs['datasource_ids']:
                if kwargs[id]:
                    kwargs[id] = "{}-{}".format(self.datasource.id_prefix, kwargs[id])
        if "id" in kwargs and self._datasource:
            kwargs["original_id"] = kwargs["id"]
            kwargs["id"] = "{}-{}".format(self.datasource.id_prefix, kwargs["id"])

        # Now that we have massaged the incoming key/value pairs, let's
        # set them in the
        bad_attributes = []
        for key, value in kwargs.items():

            if not hasattr(self, key):
                bad_attributes.append(key)
            else:
                # If enum, get value
                setattr(self, key, isinstance(value, enum.Enum) and value.value or value)

        if len(bad_attributes) > 0:
            raise ValueError("Invalid argument(s) for {} : {}".format(self.__class__.__name__,
                                                                      ",".join(bad_attributes)))

        def __setattr__(self, *ignore_args):
            """
            This has been disabled.  The class is immutable

            :param ignore_args:
            :return:
            """
            raise AttributeError("{} is Immutable".format(self.__class__.__name__))

        def __delattr__(self, *ignore_args):
            """
               This has been disabled.  The class is immutable

               :param ignore_args:
               :return:
               """
            raise AttributeError("{} is Immutable".format(self.__class__.__name__))

        self.__setattr__ = __setattr__
        self.__delattr__ = __delattr__

    def _create_mapped_attributes(self, attr_type, attr_mappings):
        """

        :param attr_type:
        :param attr_mappings:
        :return:
        """

        def create_mapped_attribute(a_type, a_mapping):
            return MappedAttribute(a_type, a_mapping)

        if isinstance(attr_mappings, list) and attr_mappings:
            return list(map(create_mapped_attribute, repeat(attr_type), attr_mappings))

        elif attr_mappings:
            return create_mapped_attribute(attr_type, attr_mappings)

    def _translate_mapped_attributes(self, plugin_access, mapped_attrs, **kwargs):
        translated_attrs = translate_attributes(plugin_access, mapped_attrs, **kwargs)
        for attr in mapped_attrs:
            if attr in translated_attrs.keys() and translated_attrs[attr]:
                translated_attrs[attr] = self._create_mapped_attributes(attr.upper(), translated_attrs[attr])
        return translated_attrs

    @property
    def datasource_ids(self):
        return self._datasource_ids

    @datasource_ids.setter
    def datasource_ids(self, value):
        self._datasource_ids = value

    @property
    def datasource(self):
        return self._datasource

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def original_id(self):
        return self._original_id

    @original_id.setter
    def original_id(self, value):
        self._original_id = value


class Person(Base):
    """A person or organization"""

    def __init__(self, **kwargs):
        self._first_name: str = None
        self._last_name: str = None
        self._email: str = None
        self._institution: str = None
        self._role: str = None

        # Initialize after the attributes have been set
        super().__init__(None, **kwargs)

    @property
    def first_name(self) -> str:
        """First (given) name of person"""
        return self._first_name

    @first_name.setter
    def first_name(self, value: str):
        self._first_name = value

    @property
    def last_name(self) -> str:
        """Last (family) name"""
        return self._last_name

    @last_name.setter
    def last_name(self, value: str):
        self._last_name = value

    @property
    def email(self) -> str:
        """Email address"""
        return self._email

    @email.setter
    def email(self, value: str):
        self._email = value

    @property
    def institution(self) -> str:
        """Institution or organization name"""
        return self._institution

    @institution.setter
    def institution(self, value: str):
        self._institution = value

    @property
    def role(self) -> str:
        """Role of person in relation to responsibility"""
        return self._role

    @role.setter
    def role(self, value: str):
        self._role = value


class Coordinate(Base):
    """
    Top level coordinate class that holds :class:`AbsoluteCoordinate` or :class:`RepresentativeCoordinate`
    """

    def __init__(self, **kwargs):
        self._absolute: AbsoluteCoordinate = None
        self._representative: RepresentativeCoordinate = None

        # Initialize after the attributes have been set
        super().__init__(None, **kwargs)
        self.__validate__()

    def __validate__(self):
        """
        Validate the attributes
        """

        # enforce absolute is class AbsoluteCoordinate
        if self.absolute and not isinstance(self.absolute, AbsoluteCoordinate):
            raise TypeError("Coordinate.absolute attribute must be AbsoluteCoordinate object")

        # enforce representative class is RepresentativeCoordinate
        if self.representative and not isinstance(self.representative, RepresentativeCoordinate):
            raise TypeError("Coordinate.representative attribute must be RepresentativeCoordinate object")

        # enforce required coordinates: if only representative, then representative.representative_point is required
        # if self.absolute is None:
        #     if self.representative.representative_point is None:
        #         raise AttributeError("Representative_point is required if only representative coordinates are provided.")

    @property
    def absolute(self) -> 'AbsoluteCoordinate':
        """Absolute coordinate"""
        return self._absolute

    @absolute.setter
    def absolute(self, value: 'AbsoluteCoordinate'):
        self._absolute = value

    @property
    def representative(self) -> 'RepresentativeCoordinate':
        """Representative coordinate"""
        return self._representative

    @representative.setter
    def representative(self, value: 'RepresentativeCoordinate'):
        self._representative = value


class AbsoluteCoordinate(Base):
    """
    Absolute coordinate describes the geo-referenced location of a feature.
    Coordinates match the feature's shape. For example, a curve is a list of points.
    Currently collections of discrete points describing a feature are supported.

    """

    # Planned extension to better check point, curve, surface, solid shape-specific coordinates.
    # May want to include a type attribute akin to GeoJSON type
    # In future, reconsider the format of attributes to allow for more types of description (meshes, solids, etc)

    def __init__(self, **kwargs):
        self._horizontal_position: List[GeographicCoordinate] = []
        self._vertical_extent: List[AltitudeCoordinate] = []

        # Initialize after the attributes have been set
        super().__init__(None, **kwargs)
        self.__validate__()

    def __validate__(self):
        # require horizontal position and vertical extent to be lists
        if not isinstance(self.horizontal_position, (list, tuple, set)):  # check for better not iterable
            self.horizontal_position = [self.horizontal_position]

        if not isinstance(self.vertical_extent, (list, tuple, set)):
            self.vertical_extent = [self.vertical_extent]

        # ToDo: validate obj types
        for obj in self.horizontal_position:
            if not isinstance(obj, GeographicCoordinate):
                raise TypeError("Horizontal position must be instance of GeographicCoordinate")

        if len(self.vertical_extent) > 0:
            for obj in self.vertical_extent:
                if not isinstance(obj, AltitudeCoordinate):
                    raise TypeError("Vertical extent must be instance of AltitudeCoordinate")

            if len(self.horizontal_position) != len(self.vertical_extent):
                raise AttributeError("Lengths of horizontal positions and vertical extent must be equal.")

        # ToDo: add validation for shape coordinates.

    @property
    def horizontal_position(self) -> List['GeographicCoordinate']:
        """list of obj :class:`GeographicCoordinate`"""
        return self._horizontal_position

    @horizontal_position.setter
    def horizontal_position(self, value: List['GeographicCoordinate']):
        self._horizontal_position = value

    @property
    def vertical_extent(self) -> List['AltitudeCoordinate']:
        """list of obj :class:`AltitudeCoordinate`"""
        return self._vertical_extent

    @vertical_extent.setter
    def vertical_extent(self, value: List['AltitudeCoordinate']):
        self._vertical_extent = value


class RepresentativeCoordinate(Base):
    """
    Representative coordinates describe the location of a feature by a representative shape / location.
    For example, a study area may be represented by the center point.
    The veritical position from a reference position (e.g., height, depth) is also described in this class.
    Currently representative points are supported. The class is extendable to other forms of representing
    (e.g., diameter, area, side_length)
    Representative point types are also expandable as use cases require.
    """

    #: Placement of the representative point is the center of a local surface
    REPRESENTATIVE_POINT_TYPE_CENTER_LOCAL_SURFACE = "CENTER LOCAL SURFACE"

    #: Placement of the representative point is the upper left corner (northwest)
    REPRESENTATIVE_POINT_TYPE_UPPER_LEFT_CORNER = "UPPER LEFT CORNER"

    #: Placement of the representative point is the upper right corner (northeast)
    REPRESENTATIVE_POINT_TYPE_UPPER_RIGHT_CORNER = "UPPER RIGHT CORNER"

    #: Placement of the representative point is the lower left corner (southhwest)
    REPRESENTATIVE_POINT_TYPE_LOWER_LEFT_CORNER = "LOWER LEFT CORNER"

    #: Placement of the representative point is the lower right corner (northeast)
    REPRESENTATIVE_POINT_TYPE_LOWER_RIGHT_CORNER = "LOWER RIGHT CORNER"

    def __init__(self, **kwargs):
        self._representative_point: AbsoluteCoordinate = None
        self._representative_point_type: str = None
        self._vertical_position: DepthCoordinate = None

        # Initialize after the attributes have been set
        super().__init__(None, **kwargs)
        self.__validate__()

    def __validate__(self):
        """
        Validate attributes
        """

        # if representative point, require representative point type
        # if self.representative_point is not None:
        #     if self.representative_point_type is None:
        #         raise AttributeError("representative_point_type is required if representative_point provided.")

    @property
    def representative_point(self) -> AbsoluteCoordinate:
        """A point representation of the feature.
           obj :class:`AbsoluteCoordinate` for POINT"""
        return self._representative_point

    @representative_point.setter
    def representative_point(self, value: AbsoluteCoordinate):
        self._representative_point = value

    @property
    def representative_point_type(self) -> str:
        """The type of representative point relative to the feature's geometry
           Currently the point is assumed to be located at the local surface (CV).
           Use constants prefixed with `REPRESENTATIVE_POINT_TYPE_` """
        return self._representative_point_type

    @representative_point_type.setter
    def representative_point_type(self, value: str):
        self._representative_point_type = value

    @property
    def vertical_position(self) -> 'DepthCoordinate':
        """The vertical position of the feature from a reference position (e.g., height or depth).
           obj :class:`DepthCoordinate`"""
        return self._vertical_position

    @vertical_position.setter
    def vertical_position(self, value: 'DepthCoordinate'):
        self._vertical_position = value


class VerticalCoordinate(Base):
    """
    The vertical position of the feature (altitudes or depths).
    The reference frame or system is specified.

    """
    #: The distance above or below sea level (elevation)
    TYPE_ALTITUDE = "ALTITUDE"

    #: The distance above (height) or below (depth) of the local surface
    TYPE_DEPTH = "DEPTH"

    #: Distance in meters
    DISTANCE_UNITS_METERS = "meters"

    #: Distance in feet
    DISTANCE_UNITS_FEET = "feet"

    #: Explicit coordinate included with horizontal coordinates
    ENCODING_EXPLICIT = "EXPLICIT"

    #: Implicit coordinate
    ENCODING_IMPLICIT = "IMPLICIT"

    #: Attribute values
    ENCODING_ATTRIBUTE = "ATTRIBUTE"

    def __init__(self, **kwargs):
        self._value: float = None
        self._resolution: float = None
        self._distance_units: str = None
        self._encoding_method: str = None
        self._datum: str = None
        self._type: str = None

        # Initialize after the attributes have been set
        super().__init__(None, **kwargs)

    @property
    def type(self) -> str:
        """The type of veritical position :class:`VerticalCoordinate.TYPE_ALTITUDE` or
           :class:`VerticalCoordinate.TYPE_DEPTH`"""
        return self._type

    @type.setter
    def type(self, value: str):
        self._type = value

    @property
    def datum(self) -> str:
        """
        The reference coordinate system. Use constants prefixed with `DATUM_`
        """
        return self._datum

    @datum.setter
    def datum(self, value: str):
        self._datum = value

    @property
    def encoding_method(self) -> str:
        """The method for encoding the units of distance. Use constants prefixed with `ENCODING_` from :class:`VerticalCoordinate`"""
        return self._distance_units

    @encoding_method.setter
    def encoding_method(self, value: str):
        self._encoding_method = value

    @property
    def distance_units(self) -> str:
        """The unit of distance. It uses constants prefixed with `DISTANCE_UNITS_` from :class:`VerticalCoordinate`"""
        return self._distance_units

    @distance_units.setter
    def distance_units(self, value: str):
        self._distance_units = value

    @property
    def resolution(self) -> float:
        """The minimum distance possible between two adjacent
           depth values, expressed in Distance Units used for Depth"""
        return self._resolution

    @resolution.setter
    def resolution(self, value: float):
        self._resolution = value

    @property
    def value(self) -> float:
        """The vertical position value"""
        return self._value

    @value.setter
    def value(self, value: float):
        self._value = value


class AltitudeCoordinate(VerticalCoordinate):
    """
    An altitudinal vertical position (i.e., distance from sea level).
    The reference frame or system is specified. The term
    "altitude" is used instead of the common term "elevation" to conform to the terminology
    in Federal Information Processing Standards 70-1 and 173.
    """

    #: National Geodetic Vertical Datum of 1929
    DATUM_NGVD29 = "NGVD29"

    #: North American Vertical Datum of 1988
    DATUM_NAVD88 = "NAVD88"

    def __init__(self, **kwargs):
        self._datum: str = None

        # Initialize after the attributes have been set
        super().__init__(type=self.TYPE_ALTITUDE, **kwargs)

    @property
    def datum(self) -> str:
        """The reference coordinate system. Use constants prefixed with `DATUM_`"""
        return self._datum

    @datum.setter
    def datum(self, value: str):
        self._datum = value


class DepthCoordinate(VerticalCoordinate):
    """
    A depth vertical position (i.e., the height or depth from the specified reference position)
    The reference frame or system is specified.
    """

    #: Local surface
    DATUM_LOCAL_SURFACE = "LS"

    #: Mean sea level
    DATUM_MEAN_SEA_LEVEL = "MSL"

    def __init__(self, **kwargs):
        self._datum = None

        # Initialize after the attributes have been set
        super().__init__(type=self.TYPE_DEPTH, **kwargs)

    @property
    def datum(self) -> str:
        """The reference coordinate system. Use constants prefixed with `DATUM_`"""
        return self._datum

    @datum.setter
    def datum(self, value: str):
        self._datum = value


class HorizontalCoordinate(Base):
    """Generic XY coordinates for a point on earth (https://www.fgdc.gov/csdgmgraphical/spref.htm)"""

    #: World Geodetic System 1984 (WGS84)
    DATUM_WGS84 = "WGS84"

    #: North American Datum of 1983 (NAD 83)
    DATUM_NAD83 = "NAD83"

    #: North American Datum 1927 (NAD27)
    DATUM_NAD27 = "NAD27"

    #: The quantities of latitude and longitude which define the position of a
    #: point on the Earth's surface with respect to a reference spheroid.
    TYPE_GEOGRAPHIC = "GEOGRAPHIC"

    #: T plane-rectangular coordinate system usually based on, and
    #: mathematically adjusted to, a map projection so that geographic
    #: positions can be readily transformed to and from plane coordinates.
    TYPE_PLANAR_GRID = "PLANAR_GRID"

    #: Any right-handed planar coordinate system of which the z-axis
    #: coincides with a plumb line through the origin that locally is aligned with the surface of the Earth.
    TYPE_PLANAR_LOCAL = "PLANAR_LOCAL"

    #: The systematic representation of all or part of the surface of the Earth on a plane or developable surface.
    TYPE_PLANAR_MAP_PROJECTION = "PLANAR_MAP_PROJECTION"

    #: A description of any coordinate system that is not aligned with the surface of the Earth.
    TYPE_LOCAL = "LOCAL"

    def __init__(self, **kwargs):
        self._x: float = None
        self._y: float = None
        self._datum: str = None
        self._type: str = None

        # Initialize after the attributes have been set
        super().__init__(None, **kwargs)

    @property
    def x(self) -> float:
        """X Coordinate"""
        return self._x

    @x.setter
    def x(self, value: float):
        self._x = value

    @property
    def y(self) -> float:
        """Y Coordinate"""
        return self._y

    @y.setter
    def y(self, value: float):
        self._y = value

    @property
    def datum(self) -> str:
        """The reference coordinate system. Use constants prefixed with `DATUM_`"""
        return self._datum

    @datum.setter
    def datum(self, value: str):
        self._datum = value

    @property
    def type(self) -> str:
        """The type of horizontal coordinates. Use constants prefixed with `TYPE_` from :class:`HorizontalCoordinate`"""
        return self._type

    @type.setter
    def type(self, value: str):
        self._type = value


class GeographicCoordinate(HorizontalCoordinate):
    """
    The latitude and longitude which define the position of a point on
    the Earth's surface with respect to a reference spheroid.
    (https://www.fgdc.gov/csdgmgraphical/spref.htm)\
    """

    #: Decimal degrees
    UNITS_DEC_DEGREES = "DD"

    #: Decimal minutes
    UNITS_DEC_MINUTES = "DM"

    #: Decimal seconds
    UNITS_DEC_SECONDS = "DS"

    #: Degrees and decimal minutes
    UNITS_DEGREES_DEC_MINUTES = "DDM"

    #: Degrees, minutes, and decimal second
    UNITS_DEGREES_MIN_DEC_SECS = "DMDS"

    #: Radians
    UNITS_RADIANS = "Radians"

    #: Grads
    UNITS_GRADS = "Grads"

    UNITS = {UNITS_DEC_DEGREES: "Decimal degrees",
             UNITS_DEC_MINUTES: "Decimal minutes",
             UNITS_DEC_SECONDS: "Decimal seconds",
             UNITS_DEGREES_DEC_MINUTES: "Degrees and decimal minutes",
             UNITS_DEGREES_MIN_DEC_SECS: "Degrees, minutes, and decimal seconds",
             UNITS_RADIANS: UNITS_RADIANS,
             UNITS_GRADS: UNITS_GRADS
             }

    UNITS_DATA_TYPES = {UNITS_DEC_DEGREES: float,
                        UNITS_DEC_MINUTES: float,
                        UNITS_DEC_SECONDS: float,
                        UNITS_DEGREES_DEC_MINUTES: (int, float),
                        UNITS_DEGREES_MIN_DEC_SECS: (int, int, int),
                        UNITS_RADIANS: float,
                        UNITS_GRADS: float
                        }

    def __init__(self, **kwargs):
        self._units: str = None

        if "longitude" in kwargs:
            kwargs["x"] = kwargs["longitude"]
            kwargs.pop("longitude")
        if "latitude" in kwargs:
            kwargs["y"] = kwargs["latitude"]
            kwargs.pop("latitude")

        # Initialize after the attributes have been set
        super().__init__(type=self.TYPE_GEOGRAPHIC, **kwargs)
        self.__validate__()

    def __validate__(self):
        """
        Validate the attributes
        """

        # Validate that the units are valid
        if self.units not in self.UNITS.keys():
            units_list = ','.join(self.UNITS.keys())
            raise AttributeError(f'{self.units} is not a valid unit. Must be in {units_list}')

        # Validate that the unit values for x and y are the correct type
        units_data_type = self.UNITS_DATA_TYPES[self.units]
        for attribute in {'x', 'y'}:
            value = getattr(self, attribute)
            if self.units and value:
                if isinstance(units_data_type, tuple):

                    if not isinstance(value, tuple) or len(units_data_type) != len(value):
                        units_data_type_list = ','.join([x.__name__ for x in units_data_type])
                        raise TypeError(f'Value {value} for {self.__class__.__name__}.{attribute} '
                                        f'must be type {type(units_data_type).__name__}'
                                        f'({units_data_type_list}')
                    else:
                        for idx, v in enumerate(value):
                            if not isinstance(v, units_data_type[idx]):
                                units_data_type_list = ','.join([x.__name__ for x in units_data_type])
                                raise TypeError(f'Value {value} for {self.__class__.__name__}.{attribute} '
                                                f'must be type {type(units_data_type).__name__}'
                                                f'({units_data_type_list}')

                # FIXME units_data_type has a redline under it.
                elif not isinstance(value, units_data_type):
                    raise TypeError(f'Value {value} for {self.__class__.__name__}.{attribute} '
                                    f'must be type {units_data_type.__name__} not '
                                    f'({type(value).__name__}')

    @property
    def latitude(self) -> float:
        """Alias for Y Coordinate"""
        return self.y

    @property
    def longitude(self) -> float:
        """Alias for X Coordinate"""
        return self.x

    @property
    def units(self) -> str:
        """Latitude and longitude units. Use constants prefixed with `UNITS_`"""
        return self._units

    @units.setter
    def units(self, value: str):
        self._units = value


class Feature(Base):
    """
    A general feature upon which an observation can be made. Loosely after GF_Feature (ISO).
    """

    def __init__(self, plugin_access, **kwargs):
        self._id: str = None
        self._name: str = None
        self._description: str = None
        self._feature_type: str = None
        self._observed_properties: Union[List[MappedAttribute], List[str]] = None

        # Initialize after the attributes have been set
        super().__init__(plugin_access, **kwargs)

        if self.observed_properties:
            if not isinstance(self.observed_properties, list):
                logger.warning("observed_properties parameter not in expected list format")
            self.observed_properties = self._create_mapped_attributes('OBSERVED_PROPERTY', get_datasource_mapped_attribute(
                plugin_access, attr_type='OBSERVED_PROPERTY', datasource_vocab=self.observed_properties))
        else:
            self.observed_properties = None

    def __validate__(self):
        """
        Validate attributes
        """

        if self.feature_type is not None and self.feature_type not in FeatureTypeEnum.values():
            raise AttributeError("Feature attr feature_type must be FeatureTypeEnum.")

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return self._id

    @property
    def id(self) -> str:
        """Unique identifier for the feature"""
        return self._id

    @id.setter
    def id(self, value: str):
        self._id = value

    @property
    def name(self) -> str:
        """A name for the feature"""
        return self._name

    @name.setter
    def name(self, value: str):
        self._name = value

    @property
    def description(self) -> str:
        """The feature description"""
        return self._description

    @description.setter
    def description(self, value: str):
        self._description = value

    @property
    def feature_type(self) -> str:
        """The feature type. For a list of feature types see :class:`basin3d.schema.enum.FeatureTypeEnum`."""
        return self._feature_type

    @feature_type.setter
    def feature_type(self, value: str):
        self._feature_type = value

    @property
    def observed_properties(self) -> Union[List[MappedAttribute], List[str]]:
        """List of observed properties"""
        return self._observed_properties

    @observed_properties.setter
    def observed_properties(self, value: Union[List[MappedAttribute], List[str]]):
        self._observed_properties = value


class SamplingFeature(Feature):
    """
    A feature where sampling is conducted. OGC Observation & Measurements SF_SamplingFeature.
    """

    def __init__(self, plugin_access, **kwargs):
        self._related_sampling_feature_complex: List[SamplingFeature] = []

        # Initialize after the attributes have been set
        super().__init__(plugin_access, **kwargs)
        self.__validate__()

    # ToDo: validate items in lists
    def __validate__(self):
        if not isinstance(self.related_sampling_feature_complex, (list, tuple, set)):  # check for better not iterable
            self.related_sampling_feature_complex = [self.related_sampling_feature_complex]

    @property
    def related_sampling_feature_complex(self) -> List['SamplingFeature']:
        """List of related sampling features
           obj :class:`RelatedSamplingFeature`"""
        return self._related_sampling_feature_complex

    @related_sampling_feature_complex.setter
    def related_sampling_feature_complex(self, value: List['SamplingFeature']):
        self._related_sampling_feature_complex = value


class SpatialSamplingFeature(SamplingFeature):
    """
    A spatially-defined feature where sampling is conducted. OGC Observation & Measurements SF_SpatialSamplingFeature.
    """

    def __init__(self, plugin_access, **kwargs):
        self._shape: str = None
        self._coordinates: Coordinate = None

        # Initialize after the attributes have been set
        super().__init__(plugin_access, **kwargs)
        self.__validate__()

        # Set the shape dependent on feature_type
        for key, values in FEATURE_SHAPE_TYPES.items():
            if self.feature_type in values:
                self.shape = key

    def __validate__(self):
        """
        Require that feature_type is set
        """

        if self.feature_type is None:
            raise AttributeError("Feature attr feature_type must be indicated")

        if self.coordinates and not isinstance(self.coordinates, Coordinate):
            raise TypeError("coordinates must be Coordinate instance.")

        self._verify_coordinates_match_shape()

    def _verify_coordinates_match_shape(self):
        # Consider: invert logic in so that the coordinates specify the shape.
        error_msg = "Absolute coordinates do not match specified shape {}. ".format(self.shape)
        if self.coordinates and self.coordinates.absolute:
            if self.shape == SpatialSamplingShapes.SHAPE_POINT:
                if len(self.coordinates.absolute.horizontal_position) != 1:
                    raise AttributeError(error_msg + "Shape {} must have only one point."
                                         .format(SpatialSamplingShapes.SHAPE_POINT))
                else:
                    return
            if self.shape == SpatialSamplingShapes.SHAPE_SURFACE:
                if len(self.coordinates.absolute.horizontal_position) < 1 or \
                        self.coordinates.absolute.horizontal_position[0].x != \
                        self.coordinates.absolute.horizontal_position[-1].x or \
                        self.coordinates.absolute.horizontal_position[0].y != \
                        self.coordinates.absolute.horizontal_position[-1].y:
                    raise AttributeError(error_msg + "Shape {} must have more than one point. "
                                                     "The first and last points in the list must "
                                                     "be the same point.".format(SpatialSamplingShapes.SHAPE_SURFACE))
                else:
                    return
            if self.shape == SpatialSamplingShapes.SHAPE_CURVE:
                if len(self.coordinates.absolute.horizontal_position) < 1 or (
                        self.coordinates.absolute.horizontal_position[0].x ==
                        self.coordinates.absolute.horizontal_position[-1].x and
                        self.coordinates.absolute.horizontal_position[0].y ==
                        self.coordinates.absolute.horizontal_position[-1].y):
                    raise AttributeError(error_msg + "Shape {} must have more than one point. "
                                                     "The first and last points in the list must "
                                                     "NOT be the same point.".format(SpatialSamplingShapes.SHAPE_CURVE))
                else:
                    return
            # ToDo: distinguish solid from curve when altitude is included

    @property
    def coordinates(self) -> Coordinate:
        """Description of feature location. An instance of :class:`Coordinate`"""
        return self._coordinates

    @coordinates.setter
    def coordinates(self, value: Coordinate):
        self._coordinates = value

    @property
    def shape(self) -> str:
        """The shape of the feature. See :class:`basin3d.models.SpatialSamplingShapes`"""
        return self._shape

    @shape.setter
    def shape(self, value: str):
        self._shape = value


class MonitoringFeature(SpatialSamplingFeature):
    """
    A feature upon which monitoring is made. OGC Timeseries Profile OM_MonitoringFeature.
    """

    def __init__(self, plugin_access, **kwargs):
        self._description_reference: str = None
        self._related_party: List[Person] = []
        self._utc_offset: int = None

        # Initialize after the attributes have been set
        super().__init__(plugin_access, **kwargs)

    @property
    def description_reference(self) -> str:
        """Extra information about the Monitoring Feature"""
        return self._description_reference

    @description_reference.setter
    def description_reference(self, value):
        self._description_reference = value

    @property
    def related_party(self) -> List[Person]:
        """list of Person, people or organizations responsible for Feature.
           To be extended in future to full OGC Responsible_Party"""
        return self._related_party

    @related_party.setter
    def related_party(self, value: List[Person]):
        self._related_party = value

    @property
    def utc_offset(self) -> int:
        """Coordinated Universal Time (UTC) offset in hours (e.g. +/-9)"""
        return self._utc_offset

    @utc_offset.setter
    def utc_offset(self, value: int):
        self._utc_offset = value


class RelatedSamplingFeature(Base):
    """
    Class that represents a related sampling feature and its role relative to
    the sampling feature to which it is related. Spatial hierarchies of features
    are built by specifying related sampling features.

    Data model from OGC Observations and Measurements.
    """

    #: Sampling Feature is a parent
    ROLE_PARENT = "PARENT"

    ROLE_TYPES = [ROLE_PARENT]

    def __init__(self, plugin_access, **kwargs):
        self._related_sampling_feature: 'SamplingFeature' = None
        self._related_sampling_feature_type: str = None
        self._role: str = None

        # Initialize after the attributes have been set
        super().__init__(plugin_access, datasource_ids=['related_sampling_feature'], **kwargs)
        self.__validate__()

    def __validate__(self):
        """
        Validate attributes
        :return:
        """
        # ToDo: refactor this to not require type
        if self.related_sampling_feature_type is not None and \
                self.related_sampling_feature_type not in FeatureTypeEnum.values():
            raise AttributeError("RelatedSamplingFeature related_sampling_feature_type must be FeatureTypeEnum")

        if self.role is None:
            raise AttributeError("RelatedSamplingFeature role is required.")
        elif self.role not in RelatedSamplingFeature.ROLE_TYPES:
            raise AttributeError("RelatedSamplingFeature role must be one of predefined roles.")

    @property
    def related_sampling_feature(self) -> 'SamplingFeature':
        """A sampling feature relation"""
        return self._related_sampling_feature

    @related_sampling_feature.setter
    def related_sampling_feature(self, value: 'SamplingFeature'):
        self._related_sampling_feature = value

    @property
    def related_sampling_feature_type(self) -> str:
        """Feature type of the related sampling feature. See :class:`FeatureTypeEnum` for a list of types"""
        return self._related_sampling_feature_type

    @related_sampling_feature_type.setter
    def related_sampling_feature_type(self, value: str):
        self._related_sampling_feature_type = value

    @property
    def role(self) -> str:
        """Currently the only Related Sampling Feature role is a :class:`RelatedSamplingFeature.PARENT`"""
        return self._role

    @role.setter
    def role(self, value: str):
        self._role = value


class TimeValuePair(namedtuple('TimeValuePair', ['timestamp', 'value'])):
    """
    Tuple that represents a time value pair.  This will handle timestamp conversion

    `(timestamp, value)`
    """

    def __new__(cls, timestamp, value):
        # Handle epoch time
        if timestamp:
            timestamp_resolved = None
            if isinstance(timestamp, str) and timestamp.isdigit():
                timestamp_resolved = int(timestamp)
            elif isinstance(timestamp, Number):
                timestamp_resolved = timestamp

            if timestamp_resolved:
                timestamp = datetime.datetime.fromtimestamp(timestamp_resolved).isoformat()

        return super().__new__(cls, timestamp, value)


class Observation(Base):
    """

    OGC OM_Observation feature type. This is a parent class to which Mixins
        should be added to create observation types with metadata and result.
    """

    #: Measurement Time Value Pair Timeseries
    TYPE_MEASUREMENT_TVP_TIMESERIES = "MEASUREMENT_TVP_TIMESERIES"

    #: A measurement
    TYPE_MEASUREMENT = "MEASUREMENT"

    def __init__(self, plugin_access, **kwargs):
        self._id: str = None
        self._type: str = None
        self._utc_offset: int = None
        self._phenomenon_time: str = None
        self._observed_property: MappedAttribute = None
        self._feature_of_interest: MonitoringFeature = None
        self._feature_of_interest_type: FeatureTypeEnum = None
        self._result_quality: List[MappedAttribute] = []

        kwargs = self._translate_attributes(plugin_access, **kwargs)

        # Initialize after the attributes have been set
        super().__init__(plugin_access, **kwargs)
        self.__validate__()

    def __eq__(self, other):
        return self.id == other.id

    def __validate__(self):
        """
        Validate attributes
        """

        # Validate feature of interest type if present is class FeatureTypeEnum
        if self.feature_of_interest_type and self.feature_of_interest_type not in FeatureTypeEnum.values():
            raise AttributeError("feature_of_interest_type must be FeatureType")

    def _translate_attributes(self, plugin_access, **kwargs):
        # ToDo: see note with TimeseriesTVPObservation
        mapped_attrs = ('observed_property', 'result_quality')
        return self._translate_mapped_attributes(plugin_access, mapped_attrs, **kwargs)

    @property
    def id(self) -> str:
        """Unique observation identifier"""
        return self._id

    @id.setter
    def id(self, value: str):
        self._id = value

    @property
    def type(self) -> str:
        """Type of observation. Use constants prefixed with `TYPE_`"""
        return self._type

    @type.setter
    def type(self, value: str):
        self._type = value

    @property
    def utc_offset(self) -> int:
        """Coordinated Universal Time (UTC) offset in hours (e.g. +/-9)"""
        return self._utc_offset

    @utc_offset.setter
    def utc_offset(self, value: int):
        self._utc_offset = value

    @property
    def phenomenon_time(self) -> str:
        """datetime of the observation (required OGC attribute timePhenomenon).
           For timeseries, start and end datetimes can be provided."""
        return self._phenomenon_time

    @phenomenon_time.setter
    def phenomenon_time(self, value: str):
        self._phenomenon_time = value

    @property
    def observed_property(self) -> 'MappedAttribute':
        """The property that was observed"""
        return self._observed_property

    @observed_property.setter
    def observed_property(self, value: 'MappedAttribute'):
        self._observed_property = value

    @property
    def feature_of_interest(self) -> 'MonitoringFeature':
        """The feature on which the observed property was observed"""
        return self._feature_of_interest

    @feature_of_interest.setter
    def feature_of_interest(self, value: 'MonitoringFeature'):
        self._feature_of_interest = value

    @property
    def feature_of_interest_type(self) -> 'FeatureTypeEnum':
        """The type of feature that was observed. See :class:`basin3d.models.FeatureTypeEnum`"""
        return self._feature_of_interest_type

    @feature_of_interest_type.setter
    def feature_of_interest_type(self, value: 'FeatureTypeEnum'):
        self._feature_of_interest_type = value

    @property
    def result_quality(self) -> List['MappedAttribute']:
        """The result quality assessment. See :class:`ResultQuality`"""
        return self._result_quality

    @result_quality.setter
    def result_quality(self, value: List['MappedAttribute']):
        self._result_quality = value


class TimeMetadataMixin(object):
    """
    Metadata attributes for Observations with a time
    """
    #: Observation taken at the start
    TIME_REFERENCE_START = "START"

    #: Observation taken in the middle
    TIME_REFERENCE_MIDDLE = "MIDDLE"

    #: Observation taken at the end
    TIME_REFERENCE_END = "END"

    def __init__(self, *args, **kwargs):
        self._aggregation_duration: MappedAttribute = None
        self._time_reference_position: str = None

        # Instantiate the serializer superclass
        super(TimeMetadataMixin, self).__init__(*args, **kwargs)

    @property
    def aggregation_duration(self) -> 'MappedAttribute':
        """Time period represented by the observation. Follows OGC TM_PeriodDuration.
           Use constants prefixed with `AGGREGATION_DURATION` from :class:`TimeseriesMetadataMixin`"""
        return self._aggregation_duration

    @aggregation_duration.setter
    def aggregation_duration(self, value: 'MappedAttribute'):
        self._aggregation_duration = value

    @property
    def time_reference_position(self) -> str:
        """Position of timestamp in aggregated_duration. Encompassed as part of OGC interpolationType.
           Use constants prefixed with `TIME_REFERENCE` from :class:`TimeseriesMetadataMixin`"""
        return self._time_reference_position

    @time_reference_position.setter
    def time_reference_position(self, value: str):
        self._time_reference_position = value


class MeasurementMetadataMixin(object):
    """
    Metadata attributes for Observations type Measurement
    """

    def __init__(self, *args, **kwargs):
        self._sampling_medium: MappedAttribute = None
        self._statistic: MappedAttribute = None

        # Instantiate the serializer superclass
        super(MeasurementMetadataMixin, self).__init__(*args, **kwargs)

    @property
    def sampling_medium(self) -> 'MappedAttribute':
        """Sampling medium in which the observed property was measured"""
        return self._sampling_medium

    @sampling_medium.setter
    def sampling_medium(self, value: 'MappedAttribute'):
        self._sampling_medium = value

    @property
    def statistic(self) -> 'MappedAttribute':
        """The statistical property of the observation result. Use constants prefixed with `STATISTIC_` from :class:`MeasurementMetadataMixin`"""
        return self._statistic

    @statistic.setter
    def statistic(self, value: 'MappedAttribute'):
        self._statistic = value


class ResultListTVP(Base):
    """
    Result Point Float
    """
    def __init__(self, plugin_access, **kwargs):
        self._value: List['TimeValuePair'] = []
        self._result_quality: List['MappedAttribute'] = []

        # translate quality
        kwargs = self._translate_attributes(plugin_access, **kwargs)

        # Initialize after the attributes have been set
        super().__init__(plugin_access, **kwargs)

    def _translate_attributes(self, plugin_access, **kwargs):
        # ToDo: see note with TimeseriesTVPObservation
        mapped_attrs = ('result_quality', )
        return self._translate_mapped_attributes(plugin_access, mapped_attrs, **kwargs)

    @property
    def value(self) -> List['TimeValuePair']:
        """Result that was measured"""
        return self._value

    @value.setter
    def value(self, value: List['TimeValuePair']):
        self._value = value

    @property
    def result_quality(self) -> List['MappedAttribute']:
        """Result that was measured"""
        return self._result_quality

    @result_quality.setter
    def result_quality(self, value: List['MappedAttribute']):
        self._result_quality = value


class MeasurementTimeseriesTVPResultMixin(object):
    """
    Result Mixin: Measurement Timeseries TimeValuePair
    """

    def __init__(self, *args, **kwargs):
        self._result: 'ResultListTVP' = None
        self._unit_of_measurement: str = None

        # Instantiate the serializer superclass
        super(MeasurementTimeseriesTVPResultMixin, self).__init__(*args, **kwargs)

    @property
    def result(self) -> 'ResultListTVP':
        """A list of results """
        return self._result

    @result.setter
    def result(self, value: 'ResultListTVP'):
        self._result = value

    @property
    def unit_of_measurement(self) -> str:
        """Unit of measurement"""
        return self._unit_of_measurement

    @unit_of_measurement.setter
    def unit_of_measurement(self, value: str):
        self._unit_of_measurement = value


class ResultPointFloat(Base):
    """
    Result Point Float
    """
    def __init__(self, plugin_access, **kwargs):
        self._value: float = None
        self._result_quality: 'MappedAttribute' = None

        # translate quality
        kwargs = self._translate_attributes(plugin_access, **kwargs)

        # Initialize after the attributes have been set
        super().__init__(None, **kwargs)

    def _translate_attributes(self, plugin_access, **kwargs):
        # ToDo: see note with TimeseriesTVPObservation
        mapped_attrs = ('result_quality', )
        return self._translate_mapped_attributes(plugin_access, mapped_attrs, **kwargs)

    @property
    def value(self) -> float:
        """Result that was measured"""
        return self._value

    @value.setter
    def value(self, value: float):
        self._value = value

    @property
    def result_quality(self) -> 'MappedAttribute':
        """Result that was measured"""
        return self._result_quality

    @result_quality.setter
    def result_quality(self, value: 'MappedAttribute'):
        self._result_quality = value


class MeasurementResultMixin(object):
    """
    Result Mixin: Measurement
    """

    def __init__(self, **kwargs):
        # self._result_value: float = None
        self._result: 'ResultPointFloat' = None
        self._unit_of_measurement: str = None

        # Instantiate the serializer superclass
        super().__init__(**kwargs)

    @property
    def result(self) -> 'ResultPointFloat':
        """Result"""
        return self._result

    @result.setter
    def result(self, value: 'ResultPointFloat'):
        self._result = value

    @property
    def unit_of_measurement(self) -> str:
        """Unit of measurement"""
        return self._unit_of_measurement

    @unit_of_measurement.setter
    def unit_of_measurement(self, value: str):
        self._unit_of_measurement = value


class MeasurementTimeseriesTVPObservation(TimeMetadataMixin, MeasurementMetadataMixin,
                                          MeasurementTimeseriesTVPResultMixin, Observation):
    """
    Series of measurement (numerical) observations in TVP format grouped by time (i.e., a timeseries).
    Anything specified at the group level automatically applies to the individual observation.
    """

    # NOTE: Position Observation (the one inheriting from Base) last in the inheritance list.
    def __init__(self, plugin_access, **kwargs):
        kwargs["type"] = self.TYPE_MEASUREMENT_TVP_TIMESERIES

        self._translate_attributes(plugin_access, **kwargs)

        # Initialize after the attributes have been set
        super(MeasurementTimeseriesTVPObservation, self).__init__(plugin_access, **kwargs)

    def __eq__(self, other):
        return self.id == other.id

    def _translate_attributes(self, plugin_access, **kwargs):
        # ToDo: Introspect which attributes are MappedAttributes. Cannot do this easily b/c using @property decorator.
        # For a simple dataclass, typing.get_type_hints would work. As properties, the fget for each property can be introspected with typing.get_type_hints
        mapped_attrs = ('observed_property', 'statistic', 'aggregation_duration', 'result_quality', 'sampling_medium')
        return self._translate_mapped_attributes(plugin_access, mapped_attrs, **kwargs)
