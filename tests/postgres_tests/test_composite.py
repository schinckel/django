"""
Testing a composite field is not easy: it needs to exist as a type
within the database, or be created in order to test it.

"""
import datetime

from django.core import exceptions, serializers
from django.db import connection
from django.test import TestCase
from django.contrib.postgres.fields import composite_type_factory

import psycopg2

from .fields import TimeBoolean, TimeBooleanField
from .models import CompositeFieldModel


class TestSimpleComposite(TestCase):
    def test_type_is_registered(self):
        from django.contrib.postgres.fields.composite import _registered_types
        assert _registered_types['time_boolean']

    def test_register_adapter(self):
        with connection.cursor() as cursor:
            cursor.execute("SELECT ('09:00', true)::time_boolean")
            value = cursor.fetchone()[0]

        assert isinstance(value, TimeBoolean)

    def test_models_create(self):
        CompositeFieldModel.objects.create(field=TimeBoolean(time=datetime.time(9), boolean=True))
        obj = CompositeFieldModel.objects.get()
        assert obj.field.time == datetime.time(9)
        assert obj.field.boolean

    def test_instantiate(self):
        obj = CompositeFieldModel()
        assert obj.field.time is None
        assert obj.field.boolean is None

        obj.field.time = '09:00'
        obj.field.boolean = False

        obj.save()

        obj = CompositeFieldModel.objects.get()

        self.assertEquals(datetime.time(9), obj.field.time)
        self.assertEquals(False, obj.field.boolean)

    def test_magic_factory(self):
        from .fields import Foo, FooField  # NOQA
        FooField()
        # We will get a database error here...
        self.assertRaises(psycopg2.ProgrammingError, Foo)
        self.assertRaises(ValueError, composite_type_factory, 'Bar', 'bar')
