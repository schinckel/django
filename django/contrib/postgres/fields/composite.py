import inspect
import sys

from django.db.models import Field
from django.db.models.base import ModelBase
from django.db import connection
from django.utils import six

from psycopg2.extras import register_composite, CompositeCaster
from psycopg2.extensions import register_adapter, adapt, AsIs
from psycopg2 import ProgrammingError

__all__ = ['CompositeType', 'composite_type_factory']

# Stash any types that have not been created yet, so
# we can register them later.
_missing_types = {}
# We also don't want to reregister types, as that is extra work
# we needn't do.
_registered_types = {}


def adapt_composite(composite):
    value = ','.join([
        adapt(getattr(composite, field.attname)).getquoted().decode('utf-8')
        for field in composite._meta.fields
    ])
    return AsIs("({})::{}".format(value, composite.db_type))


class CompositeMeta(ModelBase):
    def __new__(cls, name, bases, attrs):
        # Always abstract.
        if 'Meta' not in attrs:
            attrs['Meta'] = type('Meta', (object,), {})

        attrs['Meta'].abstract = True

        new_class = super(CompositeMeta, cls).__new__(cls, name, bases, attrs)

        # We only want to register subclasses of CompositeType, not the
        # CompositeType class itself.
        parents = [b for b in bases if isinstance(b, CompositeMeta)]
        if parents:
            new_class.register_composite()

        # We also want to create a Field subclass.
        field = type('{}Field'.format(name), (BaseCompositeField,), {
            'db_type': lambda self, connection: new_class.db_type,
            'python_type': new_class,
        })

        setattr(sys.modules[new_class.__module__], field.__name__, field)
        new_class._field = field

        return new_class

    def add_to_class(cls, name, value):
        if not inspect.isclass(value) and hasattr(value, 'contribute_to_class'):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)

    def register_composite(cls):
        db_type = cls.db_type

        class Caster(CompositeCaster):
            def make(self, values):
                return cls(**dict(zip(self.attnames, values)))

        try:
            _registered_types[db_type] = register_composite(
                db_type,
                connection.cursor().cursor,
                globally=True,
                factory=Caster
            )
        except ProgrammingError as exc:
            # This probably means it hasn't been created in the
            # database yet. Perhaps we should raise a warning?
            # We might be able to create it, but that would require
            # us to know the db connection.
            _missing_types[db_type] = (cls, exc)
        else:
            register_adapter(cls, adapt_composite)
            _missing_types.pop(db_type)


class BaseCompositeField(Field):
    def __init__(self, *args, **kwargs):
        # If the type has not yet been registered with psycopg2
        # then we want to do so. This action will remove it from
        # _missing_types, but what if it still fails? We probably
        # want to flag some type of warning in that situation.
        if self.python_type.db_type in _missing_types:
            self.python_type.register_composite()
        super(BaseCompositeField, self).__init__(*args, **kwargs)

    def get_default(self):
        return self.python_type()

    def deconstruct(self):
        name, path, args, kwargs = super(BaseCompositeField, self).deconstruct()
        path = path.replace('django.contrib.postgres.fields.composite', self.python_type.__module__)
        return name, path, args, kwargs

    def to_python(self, value):
        if isinstance(value, self.python_type):
            return value

        if value is None:
            return value

        return self.python_type(value)


class CompositeType(six.with_metaclass(CompositeMeta)):
    db_type = None

    def __init__(self, *args, **kwargs):
        # I guess it's actually now that we care about
        # if this object is not in the database.
        if self.db_type is None:
            raise ValueError('You must set a db_type')

        if self.db_type in _missing_types:
            self.__class__.register_composite()
            if self.db_type in _missing_types:
                raise _missing_types[self.db_type][1]

        fields_iter = iter(self._meta.fields)
        for val, field in zip(args, fields_iter):
            setattr(self, field.attname, val)
            kwargs.pop(field.name, None)

        for field in fields_iter:
            if kwargs:
                try:
                    val = kwargs.pop(field.attname)
                except KeyError:
                    # See django issue #12057: don't eval get_default() unless
                    # necessary.
                    val = field.get_default()
            else:
                val = field.get_default()

            setattr(self, field.attname, val)

        super(CompositeType, self).__init__(*args, **kwargs)

    __unicode__ = adapt_composite


def composite_type_factory(name, db_type, **fields):
    frame = inspect.stack()[1][0]
    if '__name__' not in frame.f_locals:
        raise ValueError('Only allowed to create classes at module level.')
    module = sys.modules[frame.f_locals['__name__']]
    assert isinstance(db_type, six.string_types), 'db_type must be a string type'
    fields['db_type'] = db_type
    fields['__module__'] = module.__name__
    setattr(module, name, type(name, (CompositeType,), fields))
