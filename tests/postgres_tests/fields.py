from django.contrib.postgres.fields import CompositeType, composite_type_factory
from django.db import models


class TimeBoolean(CompositeType):
    time = models.TimeField()
    boolean = models.BooleanField()

    db_type = 'time_boolean'

composite_type_factory('Foo', 'foo', time=models.TimeField())
