"""
..currentmodule:: basin3d.core.sqlalchemy_models

:platform: Unix, Mac
:synopsis: SQLAlchemy models for BASIN-3D data sources
:module author: Valerie C. Hendrix <vchendrix@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top

"""
from typing import Any

from sqlalchemy import Column, Integer, String, ForeignKey, JSON, UniqueConstraint, Index
from sqlalchemy import create_engine
from sqlalchemy.types import Text
from sqlalchemy.orm import relationship, sessionmaker, declarative_base

Base: Any = declarative_base()
"""The base class for all SQLAlchemy models"""


class DataSource(Base):
    """
    Data source model for BASIN-3D data sources

    Fields:
        - *id:* autoincrement integer, primary key
        - *plugin_id:* string, unique identifier for the data source
        - *name:* string, unique name for the data source
        - *id_prefix:* string, prefix that is added to all data source ids
        - *location:* string, location of the data source (e.g., URL)
        - *plugin_module:* string, module name of the data source plugin
        - *plugin_class:* string, class name of the data source plugin
    """
    __tablename__ = 'data_source'
    id = Column(Integer, primary_key=True, autoincrement=True)
    plugin_id = Column(String(50), unique=True, nullable=False)
    name = Column(String(20), unique=True, nullable=False)
    id_prefix = Column(String(5), unique=True, nullable=False)
    location = Column(Text, nullable=True)
    plugin_module = Column(Text, nullable=True)
    plugin_class = Column(Text, nullable=True)

    __table_args__ = (
        Index('idx_id_prefix', 'id_prefix'),  # Adding an index on id_prefix
    )

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<DataSource %r>' % self.name


class ObservedProperty(Base):
    """
    Observed property model for BASIN-3D data sources

    Fields:
        - *basin3d_vocab:* string, unique identifier for the observed property
        - *full_name:* string, full name of the observed property
        - *categories:* string, delimited string of categories
        - *units:* string, units of the observed property
    """
    __tablename__ = 'observed_property'
    basin3d_vocab = Column(String(50), primary_key=True, nullable=False)
    full_name = Column(String(255), nullable=False)
    categories = Column(Text, nullable=True)  # Assuming categories are stored as a delimited string
    units = Column(String(50), nullable=False)

    __table_args__ = (
        Index('idx_basin3d_vocab', 'basin3d_vocab'),  # Adding an index on id_prefix
    )

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return self.description

    def __repr__(self):
        return '<ObservedProperty %r>' % self.basin3d_vocab


class AttributeMapping(Base):
    """
    Table to map attributes between BASIN-3D and data sources

    Fields:
        - *attr_type:* string, type of attribute
        - *basin3d_vocab:* string, unique identifier for the attribute
        - *basin3d_desc:* JSON, description of the attribute
        - *datasource_vocab:* string, unique identifier for the data source attribute
        - *datasource_desc:* text, description of the data source attribute
        - *datasource_id:* integer, foreign key to the data source

    """
    __tablename__ = 'attribute_mapping'
    id = Column(Integer, primary_key=True, autoincrement=True)
    attr_type = Column(String(50), nullable=False)
    basin3d_vocab = Column(String(50), nullable=False)
    basin3d_desc = Column(JSON, nullable=False)
    datasource_vocab = Column(String(50), nullable=False)
    datasource_desc = Column(Text, nullable=True)
    datasource_id = Column(Integer, ForeignKey('data_source.id'), nullable=False)
    datasource = relationship('DataSource')

    __table_args__ = (
        UniqueConstraint('datasource_id', 'attr_type', 'datasource_vocab', name='_datasource_attr_type_vocab_uc'),
        Index('idx_datasource_attr_type', 'datasource_id', 'attr_type', 'datasource_vocab'),
        # Adding an index on id_prefix
    )

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return self.datasource_vocab


def clear_database():
    """
    Clear the database
    """
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


# Database setup in memory
# Create a temporary in memory SQLite database
engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
